-module(gblob_server_tests).
-include_lib("eunit/include/eunit.hrl").
-include("sblob.hrl").
-include("gblob.hrl").

usage_test_() ->
    ?debugMsg("starting gblob server usage tests"),
    {foreach,
     fun usage_start/0,
     fun usage_stop/1,
     [fun do_nothing/1,
      fun write_100/1,
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
    {ok, Gblob} = gblob_server:start(Path, [{max_items, 10}], ServerOpts),
    Gblob.

cleanup_on_inactivity_test() ->
    Server = new_gblob_server([{check_interval_ms, 1}]),
    timer:sleep(500),
    {Active, LastActivity} = gblob_server:status(Server),
    ?assertEqual(false, Active),
    ?assertEqual(true, LastActivity > 0).

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

