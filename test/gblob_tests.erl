-module(gblob_tests).
-include_lib("eunit/include/eunit.hrl").
-include("sblob.hrl").
-include("gblob.hrl").

usage_test_() ->
    ?debugMsg("starting gblob usage tests"),
    {foreach,
     fun usage_start/0,
     fun usage_stop/1,
     [fun do_nothing/1,
      fun only_reopen/1,
      fun write_100/1,
      fun write_100_read_1/1,
      fun write_100_read_100/1,
      fun write_100_read_first_50/1,
      fun write_100_read_middle_50/1,
      fun write_100_read_last_50/1,
      fun write_100_read_some_50/1,
      fun write_100_read_some/1,
      fun write_rotate_reopen_write/1,
      fun write_rotate_write_reopen_write/1
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
    lists:foldl(fun (I, GblobIn) ->
                       Data = num_to_data(I),
                       {GblobOut, _} = gblob:put(GblobIn, Data),
                       GblobOut
               end, Gblob, lists:seq(0, Count - 1)).

write_100(Gblob) ->
    Gblob1 = write_many(Gblob, 99),
    {_Gblob2, [E1, E2, E3, E4, E5, E6, E7, E8, E9]} = gblob:get(Gblob1, 91, 10),
    [assert_entry(E1, <<"item 90">>, 91),
     assert_entry(E2, <<"item 91">>, 92),
     assert_entry(E3, <<"item 92">>, 93),
     assert_entry(E4, <<"item 93">>, 94),
     assert_entry(E5, <<"item 94">>, 95),
     assert_entry(E6, <<"item 95">>, 96),
     assert_entry(E7, <<"item 96">>, 97),
     assert_entry(E8, <<"item 97">>, 98),
     assert_entry(E9, <<"item 98">>, 99)].

write_100_read_1(Gblob) ->
    Gblob1 = write_many(Gblob, 100),
    {_Gblob2, E1} = gblob:get(Gblob1, 48),
    [assert_entry(E1, <<"item 47">>, 48)].

read_N(Gblob, StartSN, ReadCount) ->
    ?debugFmt("read ~p start at ~p~n", [ReadCount, StartSN]),
    {Gblob1, Result} = gblob:get(Gblob, StartSN, ReadCount),
    Indexes = lists:seq(StartSN - 1, StartSN + ReadCount - 2),
    Items = lists:zip(Indexes, Result),
    {Gblob1, lists:map(fun ({I, Entity}) ->
                      Data = num_to_data(I),
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

write_100_read_some(Gblob) ->
    Gblob1 = write_many(Gblob, 100),
    {Tests, _GblobEnd} = lists:mapfoldl(fun (Count, GblobIn) ->
                      {GblobOut, Tests} = read_N(GblobIn, Count * 2, 10 + Count),
                      {Tests, GblobOut}
              end, Gblob1, lists:seq(1, 10)),
    Tests.

assert_entry(#sblob_entry{data=Data, seqnum=SeqNum, len=Len}, EData, ESeqNum) ->
    [?_assertEqual(EData, Data),
     ?_assertEqual(ESeqNum, SeqNum),
     ?_assertEqual(size(Data), Len)].

write_rotate_reopen_write(Gblob) ->
    Gblob1 = write_many(Gblob, 10),
    Gblob2 = reopen(Gblob1),
    Gblob3 = write_many(Gblob2, 5),
    {_Gblob4, [E1, E2, E3, E4, E5]} = gblob:get(Gblob3, 11, 5),
    [assert_entry(E1, <<"item 0">>, 11),
     assert_entry(E2, <<"item 1">>, 12),
     assert_entry(E3, <<"item 2">>, 13),
     assert_entry(E4, <<"item 3">>, 14),
     assert_entry(E5, <<"item 4">>, 15)].

write_rotate_write_reopen_write(Gblob) ->
    Gblob1 = write_many(Gblob, 11),
    Gblob2 = reopen(Gblob1),
    Gblob3 = write_many(Gblob2, 5),
    {_Gblob4, [E1, E2, E3, E4, E5]} = gblob:get(Gblob3, 11, 5),
    [assert_entry(E1, <<"item 10">>, 11),
     assert_entry(E2, <<"item 0">>, 12),
     assert_entry(E3, <<"item 1">>, 13),
     assert_entry(E4, <<"item 2">>, 14),
     assert_entry(E5, <<"item 3">>, 15)].

open_test() ->
    Path = "gblob",
    AbsPath = filename:absname(Path),
    Gblob = gblob:open(Path, []),
    Idx = Gblob#gblob.index,
    true = filelib:is_dir(Path),
    ?assertEqual(#gblob{path=AbsPath, index=Idx, config=#gblob_cfg{}}, Gblob).

get_blob_indexes_test() ->
    Indexes = gblob_util:get_blob_indexes_from_list(["sblob.5", "sblob.3", "sblob.4", "sblob.10"]),
    ?assertEqual(Indexes, [3, 4, 5, 10]).
