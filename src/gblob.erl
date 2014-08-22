-module(gblob).

-export([open/2, close/1, delete/1, put/2, put/3, get/2, get/3]).

-include_lib("eunit/include/eunit.hrl").
-include("gblob.hrl").
-include("sblob.hrl").

open(Path, Opts) ->
    AbsPath = filename:absname(Path),
    Config = gblob_util:parse_config(Opts),
    ok = filelib:ensure_dir(filename:join([AbsPath, "sblob"])),
    {FirstIdx, LastIdx} = gblob_util:get_blob_index_limits(AbsPath),
    #gblob{path=AbsPath, min_chunk_num=FirstIdx, max_chunk_num=LastIdx,
          config=Config}.

close(#gblob{current=nil}=Gblob) ->
    Gblob;

close(#gblob{current=Sblob}=Gblob) ->
    _Sblob1 = sblob:close(Sblob),
    Gblob#gblob{current=nil}.

delete(#gblob{path=Path}=Gblob) ->
    NewGblob = close(Gblob),
    ok = sblob_util:remove(Path),
    NewGblob.

put(Gblob, Data) ->
    Now = sblob_util:now(),
    put(Gblob, Now, Data).

put(Gblob, Timestamp, Data) ->
    {Gblob1, Sblob} = gblob_util:get_current(Gblob),
    {Sblob1, Entry} = sblob:put(Sblob, Timestamp, Data),
    Gblob2 = Gblob1#gblob{current=Sblob1},
    ShouldRotate = gblob_util:should_rotate(Gblob2),
    Gblob3 = if
        ShouldRotate -> rotate(Gblob2);
        true -> Gblob2
    end,
    {Gblob3, Entry}.

rotate(#gblob{current=Sblob, max_chunk_num=ChunkNum}=Gblob) ->
    Sblob1 = sblob:close(Sblob),
    NewChunkNum = ChunkNum + 1,
    #sblob{seqnum=LastSeqNum, fullpath=SblobPath} = Sblob1,
    NewPath = gblob_util:path_for_chunk_num(Gblob, NewChunkNum),
    ok = file:rename(SblobPath, NewPath),
    Gblob1 = Gblob#gblob{max_chunk_num=NewChunkNum, current=nil},
    {Gblob2, _NewCurrent} = gblob_util:get_current(Gblob1, LastSeqNum),
    Gblob2.

get(Gblob, SeqNum) ->
    sblob_util:handle_get_one(get(Gblob, SeqNum, 1)).

% TODO: make it span more than on sblob
get(Gblob, SeqNum, Count) ->
    {Gblob1, Sblob} = gblob_util:get_current(Gblob),
    BaseSeqNum = Sblob#sblob.base_seqnum,
    SeqNumIsAfter = (SeqNum >= BaseSeqNum),
    true = SeqNumIsAfter,
    {Sblob1, Result} = sblob:get(Sblob, SeqNum, Count),
    {Gblob1#gblob{current=Sblob1}, Result}.
