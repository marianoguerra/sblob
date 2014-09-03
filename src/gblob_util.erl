-module(gblob_util).
-export([parse_config/1,
         get_current/1, get_current/2,
         should_rotate/1,
         path_for_chunk_num/2,
         seqread/3,
         fold/4,
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
    {NewGblob, Result, NewSeqNum, ReadCount} =
        case sblob_util:seqread(Path, ChunkName, SeqNum, Count, ReadAhead) of
            {R1, nil, nil, Rc1} ->
                {Gblob1, R1, SeqNum + Rc1, Rc1};
            {R2, FirstSeqNum, _LastSeqNum, Rc2} ->
                NewIndex = sblob_idx:put(Index, ChunkNum, FirstSeqNum),
                Gblob2 = Gblob1#gblob{index=NewIndex},
                {Gblob2, R2, SeqNum + Rc2, Rc2}
        end,
    NewAccum = [Result|Accum],
    NewCount = Count - ReadCount,
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

seqread(#gblob{index=Idx}=Gblob, SeqNum, Count) ->
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
            ResStop
    end.

% like lists:foldl but the result of Fun is a tagged tuple that indicates
% if it should continue or stop
% Fun = fun((Elem :: T, AccIn) -> Res)
% Res = {stop, AccOut} | {continue, AccOut}
% it will return a tagged tuple with one of {stop, AccEnd} | {eof, AccEnd}
% depending on how it stopped processing
fold(Gblob=#gblob{path=Path, min_chunk_num=nil, max_chunk_num=nil},
     Opts, Fun, Acc0) ->
    {MinChunkNum, MaxChunkNum} = get_blob_index_limits(Path),
    fold(Gblob#gblob{min_chunk_num=MinChunkNum, max_chunk_num=MaxChunkNum},
         Opts, Fun, Acc0);

fold(#gblob{path=Path, min_chunk_num=MinChunkNum, max_chunk_num=MaxChunkNum},
     Opts, Fun, Acc0) ->

    Nums = lists:seq(MinChunkNum, MaxChunkNum),
    do_fold(fun (ChunkNum, AccIn) ->
                    ChunkName = chunk_name(ChunkNum),
                    sblob_util:fold(Path, ChunkName, Opts, Fun, AccIn)
            end, Acc0, Nums).
