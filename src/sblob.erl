-module(sblob).

-export([open/3, close/1, delete/1, put/2, put/3, get/2, get/3]).

-export([get_first_in_current/1, get_last_in_current/1]).

-include_lib("eunit/include/eunit.hrl").
-include("sblob.hrl").

open(Path, Name, Opts) ->
    Config = sblob_util:parse_config(Opts),
    AbsPath = filename:absname(Path),
    FullPath = filename:join([AbsPath, Name]),

    #sblob_cfg{base_seqnum=BaseSeqnum, max_items=MaxItems} = Config,
    Index = sblob_idx:new(BaseSeqnum, MaxItems),

    % join it with something so all the dirs to the last part are created
    ok = filelib:ensure_dir(filename:join([FullPath, "a"])),
    Sblob = #sblob{path=AbsPath, fullpath=FullPath, name=Name, config=Config,
           index=Index},

    sblob_util:fill_size(Sblob).


close(#sblob{handle=nil}=Sblob) ->
    Sblob;
close(#sblob{handle=Handle}=Sblob) ->
    file:close(Handle),
    Sblob#sblob{handle=nil}.

delete(#sblob{fullpath=FullPath}=Sblob) ->
    NewSblob = close(Sblob),
    ok = sblob_util:remove_folder(FullPath),
    NewSblob.

put(Sblob, Data) ->
    Now = sblob_util:now(),
    put(Sblob, Now, Data).

put(#sblob{seqnum=SeqNum, index=Index, size=Size}=Sblob, Timestamp, Data) ->
    {Handle, Sblob1} = sblob_util:get_handle(Sblob),
    Blob = sblob_util:to_binary(Timestamp, SeqNum, Data),
    ok = file:write(Handle, Blob),

    NewIndex = sblob_idx:put(Index, SeqNum, Size),
    BlobSize = size(Blob),
    NewSize = Size + BlobSize,

    NewSeqNum = SeqNum + 1,
    Entry = #sblob_entry{timestamp=Timestamp, seqnum=SeqNum, len=size(Data),
                         data=Data, size=BlobSize},

    Sblob2 = Sblob1#sblob{seqnum=NewSeqNum, index=NewIndex, size=NewSize},

    {Sblob2, Entry}.

get_next(Sblob) ->
    {Sblob1, {ok, Header}} = sblob_util:read(Sblob, ?SBLOB_HEADER_SIZE_BYTES),
    HeaderEntry = sblob_util:header_from_binary(Header),
    Len = HeaderEntry#sblob_entry.len,
    {Sblob2, {ok, Data}} = sblob_util:read(Sblob1, Len),
    Entry = HeaderEntry#sblob_entry{data=Data},
    {Sblob2, Entry}.

get_first_in_current(Sblob) ->
    NewSblob = sblob_util:seek(Sblob, bof),
    get_next(NewSblob).

get_last_in_current(Sblob) ->
    LenSize = ?SBLOB_HEADER_LEN_SIZE_BYTES,

    Sblob1 = sblob_util:seek(Sblob, {eof, -LenSize}),
    {Sblob2, LenData} = sblob_util:read(Sblob1, LenSize),
    {ok, <<EntryDataLen:?SBLOB_HEADER_LEN_SIZE_BITS/integer>>} = LenData,

    Offset = sblob_util:blob_size(EntryDataLen),

    Sblob3 = sblob_util:seek(Sblob2, {cur, -Offset}),
    get_next(Sblob3).

read_until(Sblob, CurSeqNum, TargetSeqNum, Accumulate) ->
    read_until(Sblob, CurSeqNum, TargetSeqNum, Accumulate, []).

read_until(Sblob, CurSeqNum, TargetSeqNum, _Accumulate, Accum)
  when CurSeqNum =:= TargetSeqNum ->
    {Sblob, CurSeqNum, Accum};

read_until(#sblob{size=Size, position=Size}=Sblob, CurSeqNum, _TargetSeqNum, _Accumulate, Accum) ->
    {Sblob, CurSeqNum, Accum};

read_until(Sblob, CurSeqNum, TargetSeqNum, Accumulate, Accum) ->
    {NewSblob, Blob} = get_next(Sblob),
    NewAccum = if Accumulate -> [Blob|Accum];
                  true -> Accum
               end,
    read_until(NewSblob, CurSeqNum + 1, TargetSeqNum, Accumulate, NewAccum).

get(Sblob, SeqNum) ->
    case get(Sblob, SeqNum, 1) of
        {NewSblob, []} -> {NewSblob, notfound};
        {NewSblob, [Entry]} -> {NewSblob, Entry}
    end.

get(#sblob{size=SblobSize}=Sblob, SeqNum, Count) ->
    % seek to closest blob before (or at) SeqNum
    {OffsetSeqnum, Sblob1} = sblob_util:seek_to_seqnum(Sblob, SeqNum),
    NewPosition = Sblob1#sblob.position,
    % advance until the start SeqNum, check if we can read the requested blobs
    %  * we are not at the end of the file
    {CanRead, Sblob2} = case OffsetSeqnum of
                % if no closest entry below SeqNum is found and we are
                % at the end of the file we can't read the entry
                nil when NewPosition =:= SblobSize -> {false, Sblob1};
                nil -> {true, Sblob1};
                Offset when Offset < SeqNum ->
                    {TSblob2, LastSeqNum, _Entries} = read_until(Sblob1, OffsetSeqnum, SeqNum, false),
                    CanReadNext = ((LastSeqNum + 1) =:= SeqNum),
                    {CanReadNext, TSblob2};
                _ -> {true, Sblob1}
            end,

    %?debugFmt("Seqnum ~p, OffsetSeqNum ~p, Position ~p, Size ~p, CanRead ~p~n",
    %          [SeqNum, OffsetSeqnum, NewPosition, SblobSize, CanRead]),
    % if can read, read the requested blobs, otherwise return empty list
    if
        CanRead ->
            {Sblob3, _, Entries} = read_until(Sblob2, SeqNum, SeqNum + Count, true),
            {Sblob3, Entries};
        true ->
            {Sblob2, []}
    end.
