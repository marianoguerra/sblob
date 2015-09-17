-module(gblob_tests).
-include_lib("eunit/include/eunit.hrl").
-include("sblob.hrl").
-include("gblob.hrl").

usage_test_() ->
    file_handle_cache:start_link(),
    ?debugMsg("starting gblob usage tests"),
    {foreach,
     fun usage_start/0,
     fun usage_stop/1,
     [fun do_nothing/1,
      fun only_reopen/1,
      fun write_expect_last_seqnum/1,
      fun write_100_empty_blobs/1,
      fun write_100_1_byte_blobs/1,
      fun write_10000_empty_blobs/1,
      fun write_10000_1_byte_blobs/1,
      fun write_100/1,
      fun write_more_than_max_items/1,
      fun write_100_read_1/1,
      fun write_100_read_100/1,
      fun write_100_read_first_50/1,
      fun write_100_read_middle_50/1,
      fun write_100_read_last_50/1,
      fun write_100_read_some_50/1,
      fun write_100_read_some/1,
      fun write_42_fold_42/1,
      fun write_42_fold_first_20/1,
      fun write_42_get_stats/1,
      fun write_42_evict_by_size/1,
      fun write_42_evict_by_current_size_percent/1,
      fun write_4_in_two_parts_read_all/1,
      fun write_100_in_two_parts_read_all/1,
      fun write_rotate_reopen_write/1,
      fun write_rotate_write_reopen_write/1,
      fun nil_seqnum_returns_last/1
     ]}.

reopen(#gblob{path=Path}=Gblob) ->
    gblob:close(Gblob),
    gblob:open(Path, [{max_items, 10}]).

usage_start() ->
    Path = io_lib:format("gblob-~p", [sblob_util:now()]),
    gblob:open(Path, [{max_items, 10}]).

usage_stop(Gblob) ->
    gblob:delete(Gblob).

do_nothing(_Gblob) -> [].

only_reopen(Gblob) ->
    reopen(Gblob),
    [].

num_to_data(Num) ->
    Str = io_lib:format("item ~p", [Num]),
    list_to_binary(Str).

write_many(Gblob, Count) ->
    write_many(Gblob, Count, 0).

write_many(Gblob, Count, Offset) ->
    lists:foldl(fun (I, GblobIn) ->
                       Data = num_to_data(I),
                       {GblobOut, _} = gblob:put(GblobIn, Data),
                       GblobOut
               end, Gblob, lists:seq(Offset, Offset + Count - 1)).

write_100(Gblob) ->
    Gblob1 = write_many(Gblob, 99),
    {_Gblob2, Entries} = gblob:get(Gblob1, 91, 10),
    ?assertEqual(9, length(Entries)),
    [E1, E2, E3, E4, E5, E6, E7, E8, E9] = Entries,
    [assert_entry(E1, <<"item 90">>, 91),
     assert_entry(E2, <<"item 91">>, 92),
     assert_entry(E3, <<"item 92">>, 93),
     assert_entry(E4, <<"item 93">>, 94),
     assert_entry(E5, <<"item 94">>, 95),
     assert_entry(E6, <<"item 95">>, 96),
     assert_entry(E7, <<"item 96">>, 97),
     assert_entry(E8, <<"item 97">>, 98),
     assert_entry(E9, <<"item 98">>, 99)].

write_100_empty_blobs(Gblob) ->
    Gblob1 = lists:foldl(fun (_I, GblobIn) ->
                                 {GblobOut, _} = gblob:put(GblobIn, <<"">>),
                                 GblobOut
                end, Gblob, lists:seq(1, 100)),
    {_Gblob2, Entries} = gblob:get(Gblob1, 91, 10),
    ?assertEqual(10, length(Entries)),
    [E1, E2, E3, E4, E5, E6, E7, E8, E9, E10] = Entries,
    [assert_entry(E1, <<"">>, 91),
     assert_entry(E2, <<"">>, 92),
     assert_entry(E3, <<"">>, 93),
     assert_entry(E4, <<"">>, 94),
     assert_entry(E5, <<"">>, 95),
     assert_entry(E6, <<"">>, 96),
     assert_entry(E7, <<"">>, 97),
     assert_entry(E8, <<"">>, 98),
     assert_entry(E9, <<"">>, 99),
     assert_entry(E10, <<"">>, 100)].


write_10000_empty_blobs(Gblob) ->
    lists:foldl(fun (_I, GblobIn) ->
                                 {GblobOut, _} = gblob:put(GblobIn, <<"">>),
                                 GblobOut
                end, Gblob, lists:seq(1, 10000)),
    [?_assertEqual(true, true)].

write_10000_1_byte_blobs(Gblob) ->
    lists:foldl(fun (_I, GblobIn) ->
                                 {GblobOut, _} = gblob:put(GblobIn, <<"b">>),
                                 GblobOut
                end, Gblob, lists:seq(1, 10000)),
    [?_assertEqual(true, true)].


write_100_1_byte_blobs(Gblob) ->
    Gblob1 = lists:foldl(fun (_I, GblobIn) ->
                                 {GblobOut, _} = gblob:put(GblobIn, <<"b">>),
                                 GblobOut
                end, Gblob, lists:seq(1, 100)),
    {_Gblob2, Entries} = gblob:get(Gblob1, 91, 10),
    ?assertEqual(10, length(Entries)),
    [E1, E2, E3, E4, E5, E6, E7, E8, E9, E10] = Entries,
    [assert_entry(E1, <<"b">>, 91),
     assert_entry(E2, <<"b">>, 92),
     assert_entry(E3, <<"b">>, 93),
     assert_entry(E4, <<"b">>, 94),
     assert_entry(E5, <<"b">>, 95),
     assert_entry(E6, <<"b">>, 96),
     assert_entry(E7, <<"b">>, 97),
     assert_entry(E8, <<"b">>, 98),
     assert_entry(E9, <<"b">>, 99),
     assert_entry(E10, <<"b">>, 100)].

write_expect_last_seqnum(Gblob) ->
    Timestamp = 42,
    Data = <<"hi there">>,
    {Gblob1, #sblob_entry{seqnum=LastSeqnum}} = gblob:put(Gblob, Timestamp, Data),
    {Gblob2, #sblob_entry{seqnum=LastSeqnum1}} = gblob:put(Gblob1, Timestamp, Data, LastSeqnum),
    {error, Reason, _Gblob3} = gblob:put(Gblob2, Timestamp, Data, LastSeqnum),
    [?_assertEqual({conflict, {seqnum, LastSeqnum1, expected, LastSeqnum}}, Reason)].

write_more_than_max_items(_Gblob) ->
    Path = io_lib:format("gblob-~p", [sblob_util:now()]),
    Offset = 0,
    Count = ?SBLOB_DEFAULT_MAX_ITEMS + 2,
    lists:foldl(fun (I, _State) ->
                        Gblob = gblob:open(Path, []),
                        Data = num_to_data(I),
                        {GblobOut, _} = gblob:put(Gblob, Data),
                        gblob:close(GblobOut),
                        nil
               end, nil, lists:seq(Offset, Offset + Count - 1)),

    Gblob1 = gblob:open(Path, []),
    {Gblob2, E1} = gblob:get(Gblob1, Count - 1),
    gblob:delete(Gblob2),
    [assert_entry(E1, <<"item 4096">>, 4097)].

write_100_read_1(Gblob) ->
    Gblob1 = write_many(Gblob, 100),
    {_Gblob2, E1} = gblob:get(Gblob1, 48),
    [assert_entry(E1, <<"item 47">>, 48)].

read_N(Gblob, StartSN, ReadCount) ->
    read_N(Gblob, StartSN, ReadCount, false).

read_N(Gblob, StartSN, ReadCount, Verbose) ->
    ?debugFmt("read ~p start at ~p~n", [ReadCount, StartSN]),
    {Gblob1, Result} = gblob:get(Gblob, StartSN, ReadCount),
    Indexes = lists:seq(StartSN - 1, StartSN + ReadCount - 2),
    Items = lists:zip(Indexes, Result),
    {Gblob1, lists:map(fun ({I, Entity}) ->
                      Data = num_to_data(I),
                      if Verbose -> ?debugFmt("~p ~p~n", [I, Entity]);
                         true -> ok
                       end,
                      assert_entry(Entity, Data, I + 1)
              end, Items)}.

write_N_read_N(Gblob, WriteCount, StartSN, ReadCount) ->
    Gblob1 = write_many(Gblob, WriteCount),
    {_Gblob2, Tests} = read_N(Gblob1, StartSN, ReadCount),
    Tests.

write_100_read_100(Gblob) ->
    write_N_read_N(Gblob, 100, 1, 100).

write_100_read_first_50(Gblob) ->
    write_N_read_N(Gblob, 100, 1, 50).

write_100_read_last_50(Gblob) ->
    write_N_read_N(Gblob, 100, 50, 50).

write_100_read_middle_50(Gblob) ->
    write_N_read_N(Gblob, 100, 25, 50).

write_100_read_some_50(Gblob) ->
    write_N_read_N(Gblob, 100, 33, 50).

write_100_in_two_parts_read_all(Gblob) ->
    Gblob1 = write_many(Gblob, 50),
    Gblob2 = write_many(Gblob1, 50, 50),
    {_Gblob3, Tests} = read_N(Gblob2, 1, 100),
    Tests.

write_4_in_two_parts_read_all(Gblob) ->
    Gblob1 = write_many(Gblob, 2),
    Gblob2 = write_many(Gblob1, 2, 2),
    {_Gblob3, Tests} = read_N(Gblob2, 1, 4),
    Tests.

write_100_read_some(Gblob) ->
    Gblob1 = write_many(Gblob, 100),
    {Tests, _GblobEnd} = lists:mapfoldl(fun (Count, GblobIn) ->
                      {GblobOut, Tests} = read_N(GblobIn, Count * 2, 10 + Count),
                      {Tests, GblobOut}
              end, Gblob1, lists:seq(1, 10)),
    Tests.

write_42_fold_42(Gblob) ->
    Gblob1 = write_many(Gblob, 42),
    Opts = [],
    {Reason, Items} = gblob_util:fold(Gblob1, Opts, fun (Entry, Entries) ->
                                          {continue, [Entry|Entries]}
                                  end, []),
    [?_assertEqual(eof, Reason),
     ?_assertEqual(42, length(Items))].

write_42_fold_first_20(Gblob) ->
    Gblob1 = write_many(Gblob, 42),
    Opts = [],
    Fun = fun (Entry, {Count, Entries}) ->
                  if Count < 20 -> {continue, {Count + 1, [Entry|Entries]}};
                     true -> {stop, Entries}
                  end
          end,
    {Reason, Items} = gblob_util:fold(Gblob1, Opts, Fun, {0, []}),
    [?_assertEqual(stop, Reason),
     ?_assertEqual(20, length(Items))].

check_eviction_size(Path, MaxSizeBytes, ExpectedSize, EToKeep, EToRemove) ->
    Resp = gblob_util:get_eviction_plan_for_size_limit(Path, MaxSizeBytes),
    {TotalSize, ToKeep, ToRemove} = Resp,

    ToKeepData = get_stats_data(ToKeep),
    ToRemoveData = get_stats_data(ToRemove),

    [?_assertEqual(ExpectedSize, TotalSize),
     ?_assertEqual(EToKeep, ToKeepData),
     ?_assertEqual(EToRemove, ToRemoveData)].

check_eviction_perc(Path, Perc, ExpectedSize, EToKeep, EToRemove) ->
    Resp = gblob_util:get_eviction_plan_for_current_size_percent(Path, Perc),
    {TotalSize, ToKeep, ToRemove} = Resp,

    ToKeepData = get_stats_data(ToKeep),
    ToRemoveData = get_stats_data(ToRemove),

    [?_assertEqual(ExpectedSize, TotalSize),
     ?_assertEqual(EToKeep, ToKeepData),
     ?_assertEqual(EToRemove, ToRemoveData)].

get_stats_data(Stats) ->
    lists:map(fun (#sblob_info{index=Index, size=Size, name=Name}) ->
                      {Index, Size, Name}
              end, Stats).

sblob_exists(#sblob_info{path=Path}) ->
    filelib:is_file(Path).

write_42_get_stats(Gblob=#gblob{path=Path}) ->
    _Gblob1 = write_many(Gblob, 42),
    {StatsTotalSize, Stats} = gblob_util:get_blobs_info(Path),
    Data = get_stats_data(Stats),
    FirstSize = 62,
    SecondSize = 300,
    OthersSize = 310,
    TotalSize = 1292,

    E0 = {0, FirstSize, "sblob"},
    E1 = {1, SecondSize, "sblob.1"},
    E2 = {2, OthersSize, "sblob.2"},
    E3 = {3, OthersSize, "sblob.3"},
    E4 = {4, OthersSize, "sblob.4"},

    % Test eviction plan here to avoid writting again

    [?_assertEqual(5, length(Stats)),
     ?_assertEqual(TotalSize, StatsTotalSize),
     ?_assertEqual([E0, E4, E3, E2, E1], Data)].

write_42_evict_by_size(Gblob=#gblob{path=Path}) ->
    _Gblob1 = write_many(Gblob, 42),
    {StatsTotalSize, Stats} = gblob_util:get_blobs_eviction_info(Path),
    FirstSize = 62,
    SecondSize = 300,
    OthersSize = 310,
    TotalSize = 1230,

    %E0 = {0, FirstSize, "sblob"},
    E1 = {1, SecondSize, "sblob.1"},
    E2 = {2, OthersSize, "sblob.2"},
    E3 = {3, OthersSize, "sblob.3"},
    E4 = {4, OthersSize, "sblob.4"},

    EvictionTests = [check_eviction_size(Path, 0, TotalSize, [],
                                         [E4, E3, E2, E1]),
                     check_eviction_size(Path, FirstSize, TotalSize, [],
                                         [E4, E3, E2, E1]),
                     check_eviction_size(Path, FirstSize + SecondSize + 150,
                                         TotalSize, [E4], [E3, E2, E1]),
                     check_eviction_size(Path, TotalSize, TotalSize,
                                         [E4, E3, E2, E1], [])],

    ExistBefore = lists:all(fun sblob_exists/1, Stats),
    Plan = gblob_util:get_eviction_plan_for_size_limit(Path, 0),
    {RemSize, RemCount, RemErrors} = gblob_util:run_eviction_plan(Plan),
    RemovedAfter = lists:any(fun sblob_exists/1, Stats),

    % Test eviction plan here to avoid writting again

    [?_assertEqual(4, length(Stats)),
     ?_assertEqual(TotalSize, StatsTotalSize),
     ?_assertEqual(true, ExistBefore),
     ?_assertEqual(false, RemovedAfter),
     ?_assertEqual(TotalSize, RemSize),
     ?_assertEqual(4, RemCount),
     ?_assertEqual([], RemErrors),
     EvictionTests].

write_42_evict_by_current_size_percent(Gblob=#gblob{path=Path}) ->
    _Gblob1 = write_many(Gblob, 42),
    {StatsTotalSize, Stats} = gblob_util:get_blobs_eviction_info(Path),
    %FirstSize = 62,
    SecondSize = 300,
    OthersSize = 310,
    TotalSize = 1230,

    %E0 = {0, FirstSize, "sblob"},
    E1 = {1, SecondSize, "sblob.1"},
    E2 = {2, OthersSize, "sblob.2"},
    E3 = {3, OthersSize, "sblob.3"},
    E4 = {4, OthersSize, "sblob.4"},

    EvictionTests = [check_eviction_perc(Path, 0, TotalSize, [],
                                         [E4, E3, E2, E1]),
                     check_eviction_perc(Path, 1, TotalSize,
                                         [E4, E3, E2, E1], []),
                     check_eviction_perc(Path, 0.5, TotalSize,
                                         [E4], [E3, E2, E1]),
                     check_eviction_perc(Path, 0.9, TotalSize,
                                         [E4, E3, E2], [E1])],

    ExistBefore = lists:all(fun sblob_exists/1, Stats),
    Plan = gblob_util:get_eviction_plan_for_current_size_percent(Path, 0.5),
    {RemSize, RemCount, RemErrors} = gblob_util:run_eviction_plan(Plan),
    RemovedAfter = lists:map(fun sblob_exists/1, Stats),

    % Test eviction plan here to avoid writting again

    [?_assertEqual(4, length(Stats)),
     ?_assertEqual(TotalSize, StatsTotalSize),
     ?_assertEqual(true, ExistBefore),
     ?_assertEqual([true, false, false, false], RemovedAfter),
     ?_assertEqual(920, RemSize),
     ?_assertEqual(3, RemCount),
     ?_assertEqual([], RemErrors),
     EvictionTests].

assert_entry(#sblob_entry{data=Data, seqnum=SeqNum, len=Len}, EData, ESeqNum) ->
    [?_assertEqual(EData, Data),
     ?_assertEqual(ESeqNum, SeqNum),
     ?_assertEqual(size(Data), Len)].

write_rotate_reopen_write(Gblob) ->
    Gblob1 = write_many(Gblob, 10),
    Gblob2 = reopen(Gblob1),
    Gblob3 = write_many(Gblob2, 5),
    {_Gblob4, Entries} = gblob:get(Gblob3, 11, 5),
    ?assertEqual(5, length(Entries)),
    [E1, E2, E3, E4, E5] = Entries,
    [assert_entry(E1, <<"item 0">>, 11),
     assert_entry(E2, <<"item 1">>, 12),
     assert_entry(E3, <<"item 2">>, 13),
     assert_entry(E4, <<"item 3">>, 14),
     assert_entry(E5, <<"item 4">>, 15)].

write_rotate_write_reopen_write(Gblob) ->
    Gblob1 = write_many(Gblob, 11),
    Gblob2 = reopen(Gblob1),
    Gblob3 = write_many(Gblob2, 5),
    {_Gblob4, Entries} = gblob:get(Gblob3, 11, 5),
    ?assertEqual(5, length(Entries)),
    [E1, E2, E3, E4, E5] = Entries,
    [assert_entry(E1, <<"item 10">>, 11),
     assert_entry(E2, <<"item 0">>, 12),
     assert_entry(E3, <<"item 1">>, 13),
     assert_entry(E4, <<"item 2">>, 14),
     assert_entry(E5, <<"item 3">>, 15)].

nil_seqnum_returns_last(Gblob) ->
    Gblob1 = write_many(Gblob, 15),
    {_Gblob2, Result} = gblob:get(Gblob1, nil, 10),
    Indexes = lists:seq(5, 14),
    Items = lists:zip(Indexes, Result),
    lists:map(fun ({I, Entity}) ->
                      Data = num_to_data(I),
                      assert_entry(Entity, Data, I + 1)
              end, Items).

open_test() ->
    Path = "gblob",
    AbsPath = filename:absname(Path),
    Gblob = gblob:open(Path, []),
    Idx = Gblob#gblob.index,
    ?assertEqual(#gblob{path=AbsPath, index=Idx, name= <<"gblob">>, config=#gblob_cfg{}}, Gblob).

get_blob_indexes_test() ->
    Indexes = gblob_util:get_blob_indexes_from_list(["sblob.5", "sblob.3", "sblob.4", "sblob.10"]),
    ?assertEqual(Indexes, [3, 4, 5, 10]).

run_emply_plan_test() ->
    {RemSize, RemCount, Errors} = gblob_util:run_eviction_plan({0, [], []}),
    ?assertEqual(0, RemSize),
    ?assertEqual(0, RemCount),
    ?assertEqual([], Errors).

fold_fun({_, _Ts, _Sn, _Ln, _Data, _Os, Sz}, CurSize) ->
    NewSize = CurSize + Sz,
    {continue, NewSize}.

fold_error_first_truncated_test() ->
    Gblob = gblob:open("../test/data/broken/first-broken-truncated", []),
    R = gblob_util:fold(Gblob, [], fun fold_fun/2, 0),
    ?assertEqual({error,{short_read,{expected,6,got,5}},217134}, R).

fold_error_second_truncated_test() ->
    Gblob = gblob:open("../test/data/broken/second-broken-truncated", []),
    R = gblob_util:fold(Gblob, [], fun fold_fun/2, 0),
    ?assertEqual({error,{short_read,{expected,32,got,31}},440938}, R).

fold_error_last_truncated_test() ->
    Gblob = gblob:open("../test/data/broken/last-broken-truncated", []),
    R = gblob_util:fold(Gblob, [], fun fold_fun/2, 0),
    ?assertEqual({error,{short_read,{expected,8,got,7}},441263}, R).
