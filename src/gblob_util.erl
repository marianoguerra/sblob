-module(gblob_util).
-export([parse_config/1,
         get_current/1, get_current/2,
         should_rotate/1,
         path_for_chunk_num/2,
         seqread/3,
         fold/4,
         get_eviction_plan_for_size_limit/2,
         get_eviction_plan_for_current_size_percent/2,
         run_eviction_plan/1,
         log_eviction_results/2,
         get_blobs_info/1,
         get_blobs_eviction_info/1,
         get_index/1,
         get_blob_indexes_from_list/1, get_blob_indexes/1,
         get_blob_index_limits/1]).

-include_lib("eunit/include/eunit.hrl").

-include("gblob.hrl").
-include("sblob.hrl").

parse_config(Opts) ->
    parse_config(Opts, #gblob_cfg{}).

parse_config([{read_ahead, Val}|T], Config) ->
    parse_config(T, Config#gblob_cfg{read_ahead=Val});

parse_config([{max_items, Val}|T], Config) ->
    parse_config(T, Config#gblob_cfg{max_items=Val});

parse_config([{max_age_ms, Val}|T], Config) ->
    parse_config(T, Config#gblob_cfg{max_age_ms=Val});

parse_config([{max_size_bytes, Val}|T], Config) ->
    parse_config(T, Config#gblob_cfg{max_size_bytes=Val});

parse_config([], Config) ->
    Config.

% nil represents the current chunk
chunk_name(nil) ->
    "sblob";

chunk_name(ChunkNum) ->
    ChunkNumStr = integer_to_list(ChunkNum),
    "sblob." ++ ChunkNumStr.

path_for_chunk_num(#gblob{path=Path}, ChunkNum) ->
    ChunkName = chunk_name(ChunkNum),
    SblobPath = filename:join([Path, ChunkName]),
    SblobPath .

sblob_for_chunk_num(Gblob, Num) ->
    sblob_for_chunk_num(Gblob, Num, []).

sblob_for_chunk_num(#gblob{path=Path}, Num, Opts) ->
    Name = chunk_name(Num),
    sblob:open(Path, Name, Opts).

get_last_seqnum(#gblob{index=nil}=Gblob) ->
    {GblobWithIndex, _Index} = get_index(Gblob),
    get_last_seqnum(GblobWithIndex);

get_last_seqnum(#gblob{max_chunk_num=MaxChunkNum}=Gblob) ->
    if
        MaxChunkNum == 0 -> 0;
        true ->
            LastSblob = sblob_for_chunk_num(Gblob, MaxChunkNum),
            Stats = sblob:stats(LastSblob),
            _ = sblob:close(LastSblob),
            Stats#sblob_stats.last_sn
    end.

get_current(Gblob) ->
    get_current(Gblob, -1).

get_current(#gblob{current=nil, path=Path}=Gblob, BaseSeqNum) ->
    ok = filelib:ensure_dir(filename:join([Path, "sblob"])),
    ActualSeqNum = if
                       BaseSeqNum == -1 -> 0;
                       true -> BaseSeqNum
                   end,
    Sblob = sblob:open(Path, "sblob", [{base_seqnum, ActualSeqNum}]),
    #sblob{size=SblobSize} = Sblob,

    Sblob1 = if
        SblobSize == 0 andalso BaseSeqNum == -1 ->
            LastSeqNum = get_last_seqnum(Gblob),
            Sblob#sblob{base_seqnum=LastSeqNum, seqnum=LastSeqNum};
        BaseSeqNum == -1 -> Sblob;
        true ->
            Sblob#sblob{base_seqnum=BaseSeqNum, seqnum=BaseSeqNum}
    end,

    {Gblob#gblob{current=Sblob1}, Sblob1};

get_current(#gblob{current=Sblob}=Gblob, _) ->
    {Gblob, Sblob}.

extract_blob_index("sblob." ++ Index) ->
    list_to_integer(Index).

get_blob_indexes_from_list(Files) ->
    Indexes = lists:map(fun extract_blob_index/1, Files),
    SortedIndexes = lists:sort(Indexes),
    SortedIndexes.

get_blob_index_limits(Path) ->
    Indexes = get_blob_indexes(Path),
    Len = length(Indexes),
    case Len of
        0 -> {0, 0};
        1 ->
            [First|_] = Indexes,
            {First, First};
        _ ->
            [First|_] = Indexes,
            Last = lists:last(Indexes),
            {First, Last}
    end.

get_blob_indexes(Path) ->
    Files = filelib:wildcard("sblob.*", Path),
    get_blob_indexes_from_list(Files).

get_blob_info_by_index(Index, BasePath) ->
    Name = "sblob." ++ integer_to_list(Index),
    sblob_util:get_blob_info(BasePath, Name, Index).

get_blobs_info(BasePath) ->
    Indexes = get_blob_indexes(BasePath),
    Fun = fun (Index) -> get_blob_info_by_index(Index, BasePath) end,
    SblobsWithIndex = lists:map(Fun, Indexes),
    CurrentStats = sblob_util:get_blob_info(BasePath, "sblob", 0),
    BlobStats = [CurrentStats|lists:reverse(SblobsWithIndex)],
    SumSizes = fun (#sblob_info{size=Size}, CurSize) -> CurSize + Size end,
    TotalSize = lists:foldl(SumSizes, 0, BlobStats),
    {TotalSize, BlobStats}.

get_blobs_eviction_info(BasePath) ->
    {TotalSize, [#sblob_info{size=CurrentSize}|Stats]} = get_blobs_info(BasePath),
    {TotalSize - CurrentSize, Stats}.

partition_by_size(Stats, MaxSizeBytes) ->
    partition_by_size(Stats, MaxSizeBytes, 0, [], []).

partition_by_size([], _MaxSizeBytes, TotalSize, ToKeep, ToRemove) ->
    {TotalSize, lists:reverse(ToKeep), lists:reverse(ToRemove)};

partition_by_size([#sblob_info{size=Size}=Stat|T], MaxSizeBytes, CurTotalSize,
                  ToKeep, ToRemove) ->

    NewCurTotalSize = CurTotalSize + Size,

    if
        NewCurTotalSize > MaxSizeBytes ->
            NewToRemove = [Stat|ToRemove],
            partition_by_size(T, MaxSizeBytes, NewCurTotalSize, ToKeep, NewToRemove);
        true ->
            NewToKeep = [Stat|ToKeep],
            partition_by_size(T, MaxSizeBytes, NewCurTotalSize, NewToKeep, ToRemove)
    end.

get_eviction_plan_for_current_size_percent(BasePath, CurSizePercentage) when CurSizePercentage =< 1 ->

    {TotalSize, Stats} = get_blobs_eviction_info(BasePath),
    MaxSizeBytes = round(TotalSize * CurSizePercentage),
    partition_by_size(Stats, MaxSizeBytes).

get_eviction_plan_for_size_limit(BasePath, MaxSizeBytes) ->
    {_TotalSize, Stats} = get_blobs_eviction_info(BasePath),
    partition_by_size(Stats, MaxSizeBytes).

evict(Path) ->
    lager:debug("Removing path ~p", [Path]),
    sblob_util:remove(Path).

% returns {RemovedSize, RemovedCount, Errors}
run_eviction_plan({_, ToKeep, ToRemove}) ->
    if length(ToRemove) > 0 ->
           lager:debug("run eviction plan keep ~p, remove ~p", [ToKeep, ToRemove]);
       true -> ok
    end,
    lists:foldl(fun (#sblob_info{path=Path, size=Size}, {CurSize, Count, Errors}) ->
                        NewErrors = try
                                        evict(Path),
                                        Errors
                                    catch
                                        _:Error -> [Error|Errors]
                                    end,

                        {CurSize + Size, Count + 1, NewErrors}
                end, {0, 0, []}, ToRemove).

log_eviction_error(Error) ->
    lager:error("eviction error ~p", [Error]).

log_eviction_results(Path, {RemovedSize, RemovedCount, Errors}) ->
    Msg = "run eviction on ~s, removed ~p blobs (~p bytes) with ~p errors",
    Args = [Path, RemovedCount, RemovedSize, length(Errors)],

    if RemovedSize > 0 -> lager:debug(Msg, Args);
       true -> lager:debug(Msg, Args)
    end,

    lists:foreach(fun log_eviction_error/1, Errors),
    ok.

should_rotate(#gblob{current=Sblob, config=Config}) ->
    #sblob_stats{size=SblobSize, count=SblobCount} = sblob:stats(Sblob),
    #gblob_cfg{max_items=MaxItems, max_size_bytes=MaxSizeBytes} = Config,
    ShouldRotate = SblobSize >= MaxSizeBytes orelse SblobCount >= MaxItems,
    ShouldRotate.

get_read_ahead_config(#gblob{current=nil}) -> ?SBLOB_DEFAULT_READ_AHEAD;
get_read_ahead_config(#sblob_cfg{read_ahead=ReadAhead}) -> ReadAhead;
get_read_ahead_config(#sblob{config=Config}) ->
    get_read_ahead_config(Config);
get_read_ahead_config(#gblob{current=Sblob}) ->
    get_read_ahead_config(Sblob).

get_index(#gblob{index=nil, path=Path}=Gblob) ->
    {FirstIdx, LastIdx} = get_blob_index_limits(Path),
    BaseFileNum = FirstIdx,
    IndexSize = LastIdx - FirstIdx + 1,
    Index = sblob_idx:new(BaseFileNum, IndexSize),
    {Gblob#gblob{min_chunk_num=FirstIdx, max_chunk_num=LastIdx, index=Index}, Index};

get_index(#gblob{index=Index}=Gblob) ->
    {Gblob, Index}.

do_seqread(Gblob, Path, ChunkNum, ChunkName, SeqNum, Count, ReadAhead, Accum) ->
    {Gblob1, Index} = get_index(Gblob),
    {RType, Gblob2, Result, NewSeqNum, ReadCount} =
        case sblob_util:seqread(Path, ChunkName, SeqNum, Count, ReadAhead) of
            {RType1, R1, nil, nil, Rc1} ->
                {RType1, Gblob1, R1, SeqNum + Rc1, Rc1};
            {RType2, R2, FirstSeqNum, _LastSeqNum, Rc2} ->
                % ignore bounds of the current chunk
                NewIndex = if ChunkNum == 0 ->
                                  Index;
                              true ->
                                  sblob_idx:put(Index, ChunkNum, FirstSeqNum)
                           end,
                RGblob2 = Gblob1#gblob{index=NewIndex},
                {RType2, RGblob2, R2, SeqNum + Rc2, Rc2};
            {error, enoent} ->
                % a sblob missing is not an error, it might be evicted
                {ok, Gblob1, [], SeqNum, 0};
            {error, Reason}=E1 ->
                lager:error("error in chunk seqread ~p ~p ~p: ~p",
                            [Path, ChunkName, ChunkNum, Reason]),
                {E1, Gblob1, [], SeqNum, 0}
        end,
    NewAccum = [Result|Accum],
    NewCount = Count - ReadCount,
    NewGblob = case RType of
                   ok -> Gblob2;
                   {error, RReason} ->
                       Gblob3 = gblob:close(Gblob2),
                       lager:warning("error in sblob:seqread, attempting recover ~p/~p.~p: ~p", 
                                     [Path, ChunkName, ChunkNum, RReason]),
                       sblob_util:recover_from_path(Path, ChunkName),
                       Gblob3
               end,
    if
        ChunkNum > 0 ->
            seqread(NewGblob, ChunkNum + 1, NewSeqNum, NewCount, ReadAhead, NewAccum);
        % if ChunkNum is zero then we are reading the current one which is
        % the last one, so we send count as 0 to finish
        true ->
            seqread(NewGblob, ChunkNum + 1, NewSeqNum, 0, ReadAhead, NewAccum)
    end.

seqread(Gblob, _ChunkNum, _SeqNum, 0, _ReadAhead, Accum) ->
    {Gblob, lists:reverse(Accum)};

% if ChunkNum is bigger than available chunks read the current one
seqread(#gblob{path=Path, max_chunk_num=MaxChunkNum}=Gblob, ChunkNum, SeqNum, Count, ReadAhead, Accum)
  when ChunkNum > MaxChunkNum ->
    do_seqread(Gblob, Path, 0, "sblob", SeqNum, Count, ReadAhead, Accum);

seqread(#gblob{path=Path}=Gblob, ChunkNum, SeqNum, Count, ReadAhead, Accum) ->
    ChunkName = chunk_name(ChunkNum),
    do_seqread(Gblob, Path, ChunkNum, ChunkName, SeqNum, Count, ReadAhead, Accum).

seqread(Gblob, SeqNum, Count) ->
    {Gblob1, Idx} = get_index(Gblob),
    BaseChunk = case sblob_idx:closest_value(Idx, SeqNum) of
        notfound -> 1;
        {ChunkNum, _} -> ChunkNum
    end,

    ReadAhead = get_read_ahead_config(Gblob1),
    Opts = [{read_ahead, ReadAhead}],
    {Gblob2, NestedResult} = seqread(Gblob1, BaseChunk, SeqNum, Count, Opts, []),
    Result = lists:flatten(NestedResult),
    {Gblob2, Result}.

do_fold(_Fun, AccIn, []) ->
    AccIn;
do_fold(Fun, AccIn, [H|T]) ->
    case Fun(H, AccIn) of
        {eof, AccOut}=ResEof ->
            IsLastItem = (length(T) == 0),
            if IsLastItem -> ResEof;
               true -> do_fold(Fun, AccOut, T)
            end;
        {stop, _}=ResStop ->
            ResStop;
        {error, _Reason, _Acc0} = Error ->
            Error
    end.

% like lists:foldl but the result of Fun is a tagged tuple that indicates
% if it should continue or stop
% Fun = fun((Elem :: T, AccIn) -> Res)
% Res = {stop, AccOut} | {continue, AccOut}
% it will return a tagged tuple with one of
% {stop, AccEnd} | {eof, AccEnd} | {error, Reason, LastAccIn}
% depending on how it stopped processing
fold(Gblob=#gblob{path=Path, min_chunk_num=nil, max_chunk_num=nil},
     Opts, Fun, Acc0) ->
    {MinChunkNum, MaxChunkNum} = get_blob_index_limits(Path),
    fold(Gblob#gblob{min_chunk_num=MinChunkNum, max_chunk_num=MaxChunkNum},
         Opts, Fun, Acc0);

fold(#gblob{path=Path, min_chunk_num=MinChunkNum, max_chunk_num=MaxChunkNum},
     Opts, Fun, Acc0) ->

    % add nil so the current chunk is also folded
    Nums = case {MinChunkNum, MaxChunkNum} of
               {A, A}  -> [nil];
               {A, B} when A == 0 -> lists:seq(A + 1, B) ++ [nil];
               {A, B} -> lists:seq(A, B) ++ [nil]
           end,
    do_fold(fun (ChunkNum, AccIn) ->
                    ChunkName = chunk_name(ChunkNum),
                    sblob_util:fold(Path, ChunkName, Opts, Fun, AccIn)
            end, Acc0, Nums).
