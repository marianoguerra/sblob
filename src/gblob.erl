-module(gblob).

-export([open/2, close/1, delete/1, put/2, put/3, get/2, get/3]).

-include_lib("eunit/include/eunit.hrl").
-include("gblob.hrl").
-include("sblob.hrl").

open(Path, Opts) ->
    AbsPath = filename:absname(Path),
    Config = gblob_util:parse_config(Opts),
    ok = filelib:ensure_dir(filename:join([AbsPath, "sblob"])),
    #gblob{path=AbsPath, config=Config}.

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

rotate(#gblob{index=nil}=Gblob) ->
    {GblobWithIndex, _Index} = gblob_util:get_index(Gblob),
    rotate(GblobWithIndex);

rotate(#gblob{current=Sblob, max_chunk_num=ChunkNum, index=Index}=Gblob) ->
    Sblob1 = sblob:close(Sblob),
    NewChunkNum = ChunkNum + 1,
    #sblob{seqnum=LastSeqNum, fullpath=SblobPath} = Sblob1,
    NewPath = gblob_util:path_for_chunk_num(Gblob, NewChunkNum),
    ok = file:rename(SblobPath, NewPath),
    NewIndex = sblob_idx:expand(Index, 1),
    Gblob1 = Gblob#gblob{max_chunk_num=NewChunkNum, current=nil, index=NewIndex},
    {Gblob2, _NewCurrent} = gblob_util:get_current(Gblob1, LastSeqNum),
    Gblob2.

get(Gblob, SeqNum) ->
    sblob_util:handle_get_one(get(Gblob, SeqNum, 1)).

get(Gblob, SeqNum, Count) ->
    {Gblob1, Sblob} = gblob_util:get_current(Gblob),
    BaseSeqNum = Sblob#sblob.base_seqnum,
    SeqNumIsAfter = (SeqNum >= BaseSeqNum),
    {Gblob2, Result} = if
        SeqNumIsAfter ->
           {Sblob1, Res} = sblob:get(Sblob, SeqNum, Count),
           {Gblob1#gblob{current=Sblob1}, Res};
        true ->
            gblob_util:seqread(Gblob, SeqNum, Count)
    end,

    {Gblob2, Result}.
