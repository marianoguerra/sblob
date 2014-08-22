-module(gblob_tests).
-include_lib("eunit/include/eunit.hrl").
-include("sblob.hrl").
-include("gblob.hrl").

usage_test_() ->
    {foreach,
     fun usage_start/0,
     fun usage_stop/1,
     [fun do_nothing/1,
      fun only_reopen/1,
      fun write_100/1,
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

write_many(Gblob, Count) ->
    lists:foldl(fun (I, GblobIn) ->
                       Str = io_lib:format("item ~p", [I]),
                       Data = list_to_binary(Str),
                       {GblobOut, _} = gblob:put(GblobIn, Data),
                       GblobOut
               end, Gblob, lists:seq(0, Count)).

write_100(Gblob) ->
    Gblob1 = write_many(Gblob, 98),
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

assert_entry(#sblob_entry{data=Data, seqnum=SeqNum, len=Len}, EData, ESeqNum) ->
    [?_assertEqual(Data, EData),
     ?_assertEqual(SeqNum, ESeqNum),
     ?_assertEqual(Len, size(Data))].

write_rotate_reopen_write(Gblob) ->
    Gblob1 = write_many(Gblob, 9),
    Gblob2 = reopen(Gblob1),
    Gblob3 = write_many(Gblob2, 5),
    {_Gblob4, [E1, E2, E3, E4, E5]} = gblob:get(Gblob3, 11, 5),
    [assert_entry(E1, <<"item 0">>, 11),
     assert_entry(E2, <<"item 1">>, 12),
     assert_entry(E3, <<"item 2">>, 13),
     assert_entry(E4, <<"item 3">>, 14),
     assert_entry(E5, <<"item 4">>, 15)].

write_rotate_write_reopen_write(Gblob) ->
    Gblob1 = write_many(Gblob, 10),
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
    true = filelib:is_dir(Path),
    ?assertEqual(#gblob{path=AbsPath, config=#gblob_cfg{}}, Gblob).

get_blob_indexes_test() ->
    Indexes = gblob_util:get_blob_indexes_from_list(["sblob.5", "sblob.3", "sblob.4", "sblob.10"]),
    ?assertEqual(Indexes, [3, 4, 5, 10]).
