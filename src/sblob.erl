-module(sblob).

-export([open/3, close/1, delete/1, put/2, put/3, get/2, get/3]).

% TODO: remove
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

    sblob_util:fill_bounds(Sblob).

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

    EntryOffset = Size,
    NewIndex = sblob_idx:put(Index, SeqNum, EntryOffset),
    BlobSize = size(Blob),
    NewSize = Size + BlobSize,

    NewSeqNum = SeqNum + 1,
    Entry = #sblob_entry{timestamp=Timestamp, seqnum=SeqNum, len=size(Data),
                         data=Data, size=BlobSize, offset=EntryOffset},

    Sblob2 = Sblob1#sblob{seqnum=NewSeqNum, index=NewIndex, size=NewSize},

    {Sblob2, Entry}.

get(Sblob, SeqNum) ->
    case get(Sblob, SeqNum, 1) of
        {NewSblob, []} -> {NewSblob, notfound};
        {NewSblob, [Entry]} -> {NewSblob, Entry}
    end.

get(Sblob, SeqNum, Count) ->
    {OffsetSeqnum, Sblob1} = sblob_util:seek_to_seqnum(Sblob, SeqNum),
    {Sblob2, LastSeqNum, _Entries} = sblob_util:read_until(Sblob1, OffsetSeqnum, SeqNum, false),
    {Sblob3, _, Entries} = sblob_util:read_until(Sblob2, LastSeqNum, SeqNum + Count, true),
    {Sblob3, Entries}.
