-module(gblob_util).
-export([parse_config/1,
         get_current/1, get_current/2,
         should_rotate/1,
         path_for_chunk_num/2,
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
    Sblob = sblob:open(Path, "sblob", [{base_seqnum, BaseSeqNum}]),
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

