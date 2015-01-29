-module(gblob).

-export([open/2, close/1, delete/1, put/2, put/3, get/2, get/3,
         check_eviction/1, truncate/2, truncate_percentage/2, size/1]).

-include_lib("eunit/include/eunit.hrl").
-include("gblob.hrl").
-include("sblob.hrl").

open(Path, Opts) when is_binary(Path) ->
    open(binary_to_list(Path), Opts);

open(Path, Opts) ->
    AbsPath = filename:absname(Path),
    Config = gblob_util:parse_config(Opts),
    Name = list_to_binary(filename:basename(Path)),
    #gblob{path=AbsPath, name=Name, config=Config}.

close(#gblob{current=nil}=Gblob) ->
    Gblob;

close(#gblob{current=Sblob}=Gblob) ->
    _Sblob1 = sblob:close(Sblob),
    Gblob#gblob{current=nil}.

delete(#gblob{path=Path}=Gblob) ->
    NewGblob = close(Gblob),
    % XXX: log remove result? (not for enoent)
    sblob_util:remove(Path),
    NewGblob.

put(Gblob, Data) ->
    Now = sblob_util:now(),
    put(Gblob, Now, Data).

put(#gblob{current=nil}=Gblob, Timestamp, Data) ->
    {Gblob1, _Sblob} = gblob_util:get_current(Gblob),
    put(Gblob1, Timestamp, Data);
put(Gblob, Timestamp, Data) ->
    ShouldRotate = gblob_util:should_rotate(Gblob),
    Gblob1 = if
        ShouldRotate -> rotate(Gblob);
        true -> Gblob
    end,
    {Gblob2, Sblob} = gblob_util:get_current(Gblob1),
    {Sblob1, Entry} = sblob:put(Sblob, Timestamp, Data),
    Gblob3 = Gblob2#gblob{current=Sblob1},
    {Gblob3, Entry}.

rotate(#gblob{index=nil}=Gblob) ->
    {GblobWithIndex, _Index} = gblob_util:get_index(Gblob),
    rotate(GblobWithIndex);

rotate(#gblob{current=Sblob, max_chunk_num=ChunkNum, index=Index}=Gblob) ->
    Sblob1 = sblob:close(Sblob),
    NewChunkNum = ChunkNum + 1,
    #sblob{seqnum=LastSeqNum, fullpath=SblobPath} = Sblob1,
    NewPath = gblob_util:path_for_chunk_num(Gblob, NewChunkNum),
    lager:debug("rotating ~p to ~p", [SblobPath, NewPath]),
    ok = file:rename(SblobPath, NewPath),
    NewIndex = sblob_idx:expand(Index, 1),
    Gblob1 = Gblob#gblob{max_chunk_num=NewChunkNum, current=nil, index=NewIndex},
    {Gblob2, _NewCurrent} = gblob_util:get_current(Gblob1, LastSeqNum),
    Gblob2.

get(Gblob, SeqNum) ->
    sblob_util:handle_get_one(get(Gblob, SeqNum, 1)).

get(Gblob=#gblob{current=nil, path=Path}, SeqNum, Count) ->
    case filelib:is_dir(Path) of
        true ->
            {Gblob1, _Sblob} = gblob_util:get_current(Gblob),
            get(Gblob1, SeqNum, Count);
        false ->
            {Gblob, []}
    end;


get(Gblob, nil, Count) ->
    {Gblob1, Sblob} = gblob_util:get_current(Gblob),
    SeqNum = Sblob#sblob.seqnum - Count + 1,

    SeqNum1 = if SeqNum < 1 -> 1;
                 true -> SeqNum
              end,

    get(Gblob1, SeqNum1, Count);

get(Gblob, SeqNum, Count) ->
    {Gblob1, Sblob} = gblob_util:get_current(Gblob),
    BaseSeqNum = Sblob#sblob.base_seqnum,
    CurrentSblobSize = Sblob#sblob.size,
    % TODO: see if the > is right
    SeqNumIsAfter = (SeqNum > BaseSeqNum andalso CurrentSblobSize > 0),
    {Gblob2, Result} = if
        SeqNumIsAfter ->
           {Sblob1, Res} = sblob:get(Sblob, SeqNum, Count),
           {Gblob1#gblob{current=Sblob1}, Res};
        true ->
           gblob_util:seqread(Gblob1, SeqNum, Count)
    end,

    {Gblob2, Result}.

size(Gblob=#gblob{path=Path}) ->
    {TotalSize, _BlobStats} = gblob_util:get_blobs_info(Path),
    {Gblob, TotalSize}.

evict(Gblob=#gblob{path=Path, config=Config, name=Name}, Plan) ->
    Result = gblob_util:run_eviction_plan(Plan),
    {RemSize, _, _} = Result,
    NewGblob = if
                   RemSize > 0 ->
                       _Gblob1 = close(Gblob),
                       #gblob{path=Path, name=Name, config=Config};
                   true -> Gblob
               end,
    {NewGblob, Result}.

truncate_percentage(Gblob=#gblob{path=Path}, Percentage) ->
    Plan = gblob_util:get_eviction_plan_for_current_size_percent(Path, Percentage),
    evict(Gblob, Plan).

truncate(Gblob=#gblob{path=Path}, MaxSizeBytes) ->
    Plan = gblob_util:get_eviction_plan_for_size_limit(Path, MaxSizeBytes),
    evict(Gblob, Plan).

check_eviction(Gblob=#gblob{config=Config}) ->
    % leave half the max size so we don't evict too often
    MaxSizeBytes = Config#gblob_cfg.max_size_bytes * 0.5,
    truncate(Gblob, MaxSizeBytes).
