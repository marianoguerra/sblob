-module(sblob).

-export([open/3, close/1, delete/1, put/2, put/3, get/2]).

-export([get_first_in_current/1, get_last_in_current/1]).

-include_lib("eunit/include/eunit.hrl").
-include("sblob.hrl").

open(Path, Name, Opts) ->
    Config = sblob_util:parse_config(Opts),
    AbsPath = filename:absname(Path),
    FullPath = filename:join([AbsPath, Name]),
    % join it with something so all the dirs to the last part are created
    ok = filelib:ensure_dir(filename:join([FullPath, "a"])),
    #sblob{path=AbsPath, fullpath=FullPath, name=Name, config=Config}.

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

put(#sblob{seqnum=SeqNum}=Sblob, Timestamp, Data) ->
    {Handle, NewSblob} = sblob_util:get_handle(Sblob),
    Blob = sblob_util:to_binary(Timestamp, SeqNum, Data),
    ok = file:write(Handle, Blob),
    NewSeqNum = SeqNum + 1,
    {NewSblob#sblob{seqnum=NewSeqNum},
     #sblob_entry{timestamp=Timestamp, seqnum=SeqNum, len=size(Data), data=Data}}.

get_next(Sblob) ->
    {Sblob1, {ok, Header}} = sblob_util:read(Sblob, ?SBLOB_HEADER_SIZE_BYTES),
    HeaderEntry = sblob_util:header_from_binary(Header),
    % TODO: don't pattern match?
    #sblob_entry{len=Len} = HeaderEntry,
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

get(Sblob, SeqNum) ->
    NewSblob = sblob_util:seek_to_seqnum(Sblob, SeqNum),
    get_next(NewSblob).
