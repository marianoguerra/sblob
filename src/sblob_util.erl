-module(sblob_util).
-export([parse_config/1, now/0,
         get_handle/1, seek/2, seek_to_seqnum/2,
         clear_data/1, read/2, remove/1,
         handle_get_one/1, seqread/5,
         get_next/1, get_first/1, get_last/1, read_until/4,
         to_binary/1, to_binary/3, from_binary/1, header_from_binary/1,
         blob_size/1, offset_for_seqnum/2, fill_bounds/1]).

-include("sblob.hrl").

now() ->
    {Mega, Sec, Micro} = erlang:now(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.

parse_config(Opts) ->
    parse_config(Opts, #sblob_cfg{}).

parse_config([{read_ahead, Val}|T], Config) ->
    parse_config(T, Config#sblob_cfg{read_ahead=Val});

parse_config([{max_items, Val}|T], Config) ->
    parse_config(T, Config#sblob_cfg{max_items=Val});

parse_config([{base_seqnum, Val}|T], Config) ->
    parse_config(T, Config#sblob_cfg{base_seqnum=Val});

parse_config([], Config) ->
    Config.

open_file(Path, ReadAhead) ->
    {ok, Handle} = file:open(Path, [append, read, raw, binary,
                                    {read_ahead, ReadAhead}]),
    Handle.

% return the file handle to the current chunk, if not open already open it
% and store it in the returned Sblob record
get_handle(#sblob{fullpath=FullPath, handle=nil,
                  config=#sblob_cfg{read_ahead=ReadAhead}}=Sblob) ->
    lager:debug("get handle ~s", [FullPath]),
    Handle = open_file(FullPath, ReadAhead),
    {ok, Size} = file:position(Handle, eof),
    {Handle, Sblob#sblob{handle=Handle, position=Size, size=Size}};

get_handle(#sblob{handle=Handle}=Sblob) ->
    {Handle, Sblob}.

% like get_handle but will seek the handle to the required place,
% Location is the same as file:position
seek(#sblob{handle=nil}=Sblob, Location) ->
    {_, Sblob1} = get_handle(Sblob),
    seek(Sblob1, Location);
seek(#sblob{handle=Handle, name=Name}=Sblob, Location) ->
    lager:debug("seek ~s ~p", [Name, Location]),
    {ok, NewPos} = file:position(Handle, Location),
    Sblob#sblob{position=NewPos}.

seek_to_seqnum(Sblob, SeqNum) ->
    {Handle, NewSblob} = get_handle(Sblob),
    {OffsetKey, Offset} =  case offset_for_seqnum(Sblob, SeqNum) of
        notfound -> {nil, 0};
        Result -> Result
    end,
    {ok, NewPos} = file:position(Handle, {bof, Offset}),
    {OffsetKey, NewSblob#sblob{position=NewPos}}.

fill_bounds(#sblob{name=Name}=Sblob) ->
    {Sblob1, Last} = get_first(Sblob),
    Cfg = Sblob1#sblob.config,
    CfgBaseSeqNum = Cfg#sblob_cfg.base_seqnum,
    case Last of
        notfound -> 
            lager:debug("fill bounds, empty ~s", [Name]),
            Sblob1#sblob{base_seqnum=CfgBaseSeqNum, seqnum=CfgBaseSeqNum, size=0};
        #sblob_entry{seqnum=FirstSeqNum} ->
            {Sblob2, #sblob_entry{seqnum=LastSeqNum}} = get_last(Sblob1),
            BaseSeqNum = FirstSeqNum  - 1,
            lager:debug("fill bounds ~s ~p - ~p", [Name, BaseSeqNum, LastSeqNum]),
            Sblob2#sblob{base_seqnum=BaseSeqNum, seqnum=LastSeqNum}
    end.

read(#sblob{handle=nil}=Sblob, Len) ->
    {_, NewSblob} = get_handle(Sblob),
    read(NewSblob, Len);

read(#sblob{position=Pos, handle=Handle}=Sblob, Len) ->
    {Sblob#sblob{position=Pos + Len}, file:read(Handle, Len)}.

to_binary(#sblob_entry{timestamp=Timestamp, seqnum=SeqNum, data=Data}) ->
    to_binary(Timestamp, SeqNum, Data).

to_binary(Timestamp, SeqNum, Data) ->
    Len = size(Data),
    <<Timestamp:64/integer, SeqNum:64/integer, Len:32/integer, Data/binary,
    Len:32/integer>>.

from_binary(<<Timestamp:64/integer, SeqNum:64/integer, Len:32/integer, Tail/binary>>) ->
    Data = binary:part(Tail, 0, Len),
    #sblob_entry{timestamp=Timestamp, seqnum=SeqNum, len=Len, data=Data,
                 size=?SBLOB_HEADER_SIZE_BYTES + size(Tail)}.

header_from_binary(<<Timestamp:64/integer, SeqNum:64/integer, Len:32/integer, _Tail/binary>>) ->
    #sblob_entry{timestamp=Timestamp, seqnum=SeqNum, len=Len, data=nil}.

clear_data(Entry) ->
    Entry#sblob_entry{data=nil}.

% returns the size of the complete blob entry in disk (header, data and footer)
blob_size(EntryDataLen) ->
    HeaderSize = ?SBLOB_HEADER_SIZE_BYTES,
    LenSize = ?SBLOB_HEADER_LEN_SIZE_BYTES,
    EntryDataLen + LenSize + HeaderSize.

remove(Path) ->
    % TODO: delete instead of move
    NowStr = integer_to_list(sblob_util:now()),
    ok = file:rename(Path, Path ++ "." ++ NowStr ++ ".removed").

offset_for_seqnum(#sblob{index=Idx}, SeqNum) -> 
    Result = sblob_idx:closest(Idx, SeqNum),
    lager:debug("offset for seqnum ~p: ~p", [SeqNum, Result]),
    Result.

raw_get_next(Handle) ->
     case file:read(Handle, ?SBLOB_HEADER_SIZE_BYTES) of
         {ok, Header} ->

             HeaderEntry = header_from_binary(Header),
             Len = HeaderEntry#sblob_entry.len,
             {ok, Tail} = file:read(Handle, Len + ?SBLOB_HEADER_LEN_SIZE_BYTES),
             Data = binary:part(Tail, 0, Len),
             Entry = HeaderEntry#sblob_entry{data=Data,
                                             size=?SBLOB_HEADER_SIZE_BYTES + size(Tail)},
             Entry;
         eof -> eof
     end.

get_next(#sblob{position=Pos, size=Pos}=Sblob) -> {Sblob, notfound};

get_next(Sblob) ->
    {Sblob1, {ok, Header}} = read(Sblob, ?SBLOB_HEADER_SIZE_BYTES),
    HeaderEntry = header_from_binary(Header),
    Len = HeaderEntry#sblob_entry.len,
    {Sblob2, {ok, Tail}} = read(Sblob1, Len + ?SBLOB_HEADER_LEN_SIZE_BYTES),
    Data = binary:part(Tail, 0, Len),
    EntryOffset = Sblob#sblob.position,
    Entry = HeaderEntry#sblob_entry{data=Data,
                                    offset=EntryOffset,
                                    size=?SBLOB_HEADER_SIZE_BYTES + size(Tail)},

    BlobSeqNum = Entry#sblob_entry.seqnum,
    NewIndex = sblob_idx:put(Sblob2#sblob.index, BlobSeqNum, EntryOffset),
    Sblob3 = Sblob2#sblob{index=NewIndex},

    {Sblob3, Entry}.

get_first(#sblob{fullpath=FullPath}=Sblob) ->
    lager:debug("get_first ~s", [FullPath]),
    NewSblob = seek(Sblob, bof),
    get_next(NewSblob).

get_last(#sblob{fullpath=FullPath}=Sblob) ->
    lager:debug("get_last ~s", [FullPath]),
    LenSize = ?SBLOB_HEADER_LEN_SIZE_BYTES,

    Sblob1 = seek(Sblob, {eof, -LenSize}),
    {Sblob2, LenData} = read(Sblob1, LenSize),
    % since we read the last 4 bytes for the entry len we are at the end,
    % that means that now position == size, we use it to set the blob size
    SblobSize = Sblob2#sblob.position,
    {ok, <<EntryDataLen:?SBLOB_HEADER_LEN_SIZE_BITS/integer>>} = LenData,

    Offset = blob_size(EntryDataLen),

    Sblob3 = seek(Sblob2#sblob{size=SblobSize}, {cur, -Offset}),
    get_next(Sblob3).


read_until(#sblob{name=Name}=Sblob, CurSeqNum, TargetSeqNum, Accumulate) ->
    lager:debug("read_until ~s ~p ~p", [Name, CurSeqNum, TargetSeqNum]),
    read_until(Sblob, CurSeqNum, TargetSeqNum, Accumulate, []).


read_until(Sblob, CurSeqNum, TargetSeqNum, _Accumulate, Accum)
  when CurSeqNum >= TargetSeqNum ->
    {Sblob, CurSeqNum, lists:reverse(Accum)};

read_until(#sblob{size=Size, position=Size}=Sblob, CurSeqNum, _TargetSeqNum, _Accumulate, Accum) ->
    {Sblob, CurSeqNum, lists:reverse(Accum)};

read_until(Sblob, CurSeqNum, TargetSeqNum, Accumulate, Accum) ->
    case get_next(Sblob) of
        {TSblob1, notfound} -> {TSblob1, CurSeqNum, lists:reverse(Accum)};
        {Sblob1, Blob} ->
            NewAccum = if Accumulate -> [Blob|Accum];
                          true -> Accum
                       end,

            read_until(Sblob1, Blob#sblob_entry.seqnum + 1, TargetSeqNum, Accumulate, NewAccum)
    end.

handle_get_one(Result) ->
    case Result of
        {First, []} -> {First, notfound};
        {First, [Entry]} -> {First, Entry}
    end.

seqread_raw(Handle, _SeqNum, FirstSeqNum, LastSeqNum, 0, Count, Accum) ->
    file:close(Handle),
    Result = lists:reverse(Accum),
    {Result, FirstSeqNum, LastSeqNum, Count};

seqread_raw(Handle, SeqNum, FirstSeqNum, LastSeqNum, Remaining, Count, Accum) ->
    case raw_get_next(Handle) of
        eof ->
            seqread_raw(Handle, SeqNum, FirstSeqNum, LastSeqNum, 0, Count, Accum);
        (#sblob_entry{seqnum=EntrySeqNum}=Entry) ->
            NewFirstSeqNum = if
                                 FirstSeqNum =:= nil -> EntrySeqNum;
                                 true -> FirstSeqNum
                             end,
            if
                EntrySeqNum >= SeqNum ->
                    seqread_raw(Handle, SeqNum, NewFirstSeqNum, EntrySeqNum,
                                Remaining - 1, Count + 1, [Entry|Accum]);
                true ->
                    seqread_raw(Handle, SeqNum, NewFirstSeqNum, EntrySeqNum,
                                Remaining, Count, Accum)
            end
    end.

seqread(Path, ChunkName, SeqNum, Count, ReadAhead) ->
    lager:debug("seqread ~s ~p ~p", [ChunkName, SeqNum, Count]),
    SblobPath = filename:join([Path, ChunkName]),
    Handle = open_file(SblobPath, ReadAhead),
    seqread_raw(Handle, SeqNum, nil, nil, Count, 0, []).

