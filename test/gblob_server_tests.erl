-module(gblob_server_tests).
-include_lib("eunit/include/eunit.hrl").
-include("sblob.hrl").
-include("gblob.hrl").

usage_test_() ->
    file_handle_cache:start_link(),
    ?debugMsg("starting gblob server usage tests"),
    {foreach,
     fun usage_start/0,
     fun usage_stop/1,
     [fun do_nothing/1,
      fun write_100/1,
      fun write_100_get_size/1,
      fun write_100_truncate/1,
      fun write_100_truncate_half/1,
      fun write_100_read_1/1,
      fun write_100_read_100/1,
      fun write_100_read_first_50/1,
      fun write_100_read_middle_50/1,
      fun write_100_read_last_50/1,
      fun write_100_read_some_50/1,
      fun write_100_read_some/1
     ]}.

new_gblob_server(ServerOpts) ->
    Path = io_lib:format("gblob-~p", [sblob_util:now()]),
    {ok, Gblob} = gblob_server:start_link(Path, [{max_items, 10}], ServerOpts),
    Gblob.

cleanup_on_inactivity_test() ->
    Server = new_gblob_server([{check_interval_ms, 1}]),
    timer:sleep(500),
    {Active, LastActivity, LastEviction} = gblob_server:status(Server),
    ?assertEqual(false, Active),
    ?assertEqual(true, LastEviction == 0),
    ?assertEqual(true, LastActivity > 0).

force_evict_on_inactivity_test() ->
    Server = new_gblob_server([{max_interval_no_eviction_ms, 1}]),
    Data1 = num_to_data(1),
    Data2 = num_to_data(2),
    gblob_server:put(Server, Data1),
    timer:sleep(100),
    gblob_server:put(Server, Data2),
    {Active, LastActivity, LastEviction} = gblob_server:status(Server),
    ?assertEqual(true, Active),
    ?assertEqual(true, LastActivity > 0),
    ?assertEqual(true, LastEviction > 0).

set_active_on_action_after_cleanup_test() ->
    Server = new_gblob_server([{check_interval_ms, 100}]),
    timer:sleep(500),
    {Active, LastActivity, _} = gblob_server:status(Server),
    ?assertEqual(false, Active),
    ?assertEqual(true, LastActivity > 0),
    Item = gblob_server:get(Server, 42),
    {Active1, LastActivity1, _} = gblob_server:status(Server),
    StopResponse = gblob_server:stop(Server),
    ?assertEqual(notfound, Item),
    ?assertEqual(true, Active1),
    ?assertEqual(true, LastActivity < LastActivity1),
    ?assertEqual(stopped, StopResponse).

usage_start() ->
    new_gblob_server([]).

usage_stop(Gblob) ->
    gblob_server:stop(Gblob).

do_nothing(_Gblob) -> [].

num_to_data(Num) ->
    Str = io_lib:format("item ~p", [Num]),
    list_to_binary(Str).

write_many(Gblob, Count) ->
    lists:foreach(fun (I) ->
                       Data = num_to_data(I),
                       gblob_server:put(Gblob, Data)
               end, lists:seq(0, Count - 1)).

write_100(Gblob) ->
    write_many(Gblob, 99),
    [E1, E2, E3, E4, E5, E6, E7, E8, E9] = gblob_server:get(Gblob, 91, 10),
    [assert_entry(E1, <<"item 90">>, 91),
     assert_entry(E2, <<"item 91">>, 92),
     assert_entry(E3, <<"item 92">>, 93),
     assert_entry(E4, <<"item 93">>, 94),
     assert_entry(E5, <<"item 94">>, 95),
     assert_entry(E6, <<"item 95">>, 96),
     assert_entry(E7, <<"item 96">>, 97),
     assert_entry(E8, <<"item 97">>, 98),
     assert_entry(E9, <<"item 98">>, 99)].

write_100_get_size(Gblob) ->
    write_many(Gblob, 100),
    Size = gblob_server:size(Gblob), 
    [?_assertEqual(3090, Size)].

write_100_truncate(Gblob) ->
    write_many(Gblob, 99),
    TruncateResult = gblob_server:truncate(Gblob, 0),
    ?debugVal(TruncateResult),
    Result = gblob_server:get(Gblob, 91, 10),
    [?_assertEqual([], Result)].

write_100_truncate_half(Gblob) ->
    write_many(Gblob, 99),
    TruncateResult = gblob_server:truncate_percentage(Gblob, 0.5),
    ?debugVal(TruncateResult),
    Result = gblob_server:get(Gblob, 0, 10),
    ?assertEqual(10, length(Result)),
    [E1, E2, E3, E4, E5, E6, E7, E8, E9, E10] = Result,
    [assert_entry(E1, <<"item 50">>, 51),
     assert_entry(E2, <<"item 51">>, 52),
     assert_entry(E3, <<"item 52">>, 53),
     assert_entry(E4, <<"item 53">>, 54),
     assert_entry(E5, <<"item 54">>, 55),
     assert_entry(E6, <<"item 55">>, 56),
     assert_entry(E7, <<"item 56">>, 57),
     assert_entry(E8, <<"item 57">>, 58),
     assert_entry(E9, <<"item 58">>, 59),
     assert_entry(E10, <<"item 59">>, 60)].

write_100_read_1(Gblob) ->
    write_many(Gblob, 100),
    E1 = gblob_server:get(Gblob, 48),
    [assert_entry(E1, <<"item 47">>, 48)].

read_N(Gblob, StartSN, ReadCount) ->
    ?debugFmt("read ~p start at ~p~n", [ReadCount, StartSN]),
    Result = gblob_server:get(Gblob, StartSN, ReadCount),
    Indexes = lists:seq(StartSN - 1, StartSN + ReadCount - 2),
    Items = lists:zip(Indexes, Result),
    lists:map(fun ({I, Entity}) ->
                      Data = num_to_data(I),
                      assert_entry(Entity, Data, I + 1)
              end, Items).

write_N_read_N(Gblob, WriteCount, StartSN, ReadCount) ->
    write_many(Gblob, WriteCount),
    read_N(Gblob, StartSN, ReadCount).

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
    write_many(Gblob, 100),
    lists:map(fun (Count) ->
                      read_N(Gblob, Count * 2, 10 + Count)
              end, lists:seq(1, 10)).

assert_entry(#sblob_entry{data=Data, seqnum=SeqNum, len=Len}, EData, ESeqNum) ->
    [?_assertEqual(EData, Data),
     ?_assertEqual(ESeqNum, SeqNum),
     ?_assertEqual(size(Data), Len)].

