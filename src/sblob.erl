-module(sblob).

-export([open/3, close/1, delete/1, put/2, put/3, get/2]).
-include("sblob.hrl").

open(Path, Name, Opts) ->
    Config = sblob_util:parse_config(Opts),
    AbsPath = filename:absname(Path),
    FullPath = filename:join([AbsPath, Name]),
    % join it with something so all the dirs to the last part are created
    ok = filelib:ensure_dir(filename:join([FullPath, "a"])),
    #sblob{path=AbsPath, fullpath=FullPath, name=Name, config=Config}.

close(#sblob{handle=Handle}=Sblob) when Handle =:= nil ->
    file:close(Handle),
    Sblob#sblob{handle=nil};
close(#sblob{handle=nil}=Sblob) ->
    Sblob.

delete(#sblob{fullpath=FullPath}=Sblob) ->
    NewSblob = close(Sblob),
    % TODO: delete instead of move
    NowStr = integer_to_list(sblob_util:now()),
    ok = file:rename(FullPath, FullPath ++ "." ++ NowStr ++ ".removed"),
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

get(#sblob{}=Sblob, SeqNum) ->
    {Handle, NewSblob} = sblob_util:get_handle(Sblob),
    Offset = sblob_util:offset_for_seqnum(Sblob, SeqNum),
    file:position(Handle, Offset),
    {ok, Header} = file:read(Handle, ?SBLOB_HEADER_SIZE_BYTES),
    HeaderEntry = sblob_util:from_binary(Header),
    % TODO: don't pattern match?
    #sblob_entry{len=Len} = HeaderEntry,
    {ok, Data} = file:read(Handle, Len),
    Entry = HeaderEntry#sblob_entry{data=Data},
    {NewSblob, Entry}.
