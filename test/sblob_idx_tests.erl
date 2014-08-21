-module(sblob_idx_tests).
-include_lib("eunit/include/eunit.hrl").
-include("sblob.hrl").

new_test() ->
    _Idx = sblob_idx:new(22).

get_notfound_test() ->
    Idx = sblob_idx:new(22),
    ?assertEqual(sblob_idx:closest(Idx, 22), notfound),
    ?assertEqual(sblob_idx:closest(Idx, 33), notfound),
    ?assertEqual(sblob_idx:closest(Idx, 24), notfound),
    ?assertEqual(sblob_idx:closest(Idx, 275), notfound).

put_get_test() ->
    Idx = sblob_idx:new(22),
    Idx1 = sblob_idx:put(Idx, 22, a),
    {Key, Val} = sblob_idx:closest(Idx1, 22),
    ?assertEqual(Key, 22),
    ?assertEqual(Val, a).

put_closest_test() ->
    Idx = sblob_idx:new(22),
    Idx1 = sblob_idx:put(Idx, 22, a),
    {Key, Val} = sblob_idx:closest(Idx1, 25),
    ?assertEqual(Key, 22),
    ?assertEqual(Val, a).

put_get_notfound_test() ->
    Idx = sblob_idx:new(22),
    Idx1 = sblob_idx:put(Idx, 25, a),
    Val = sblob_idx:closest(Idx1, 24),
    ?assertEqual(Val, notfound).

put_get_out_of_bounds_upper_test() ->
    Idx = sblob_idx:new(22),
    Idx1 = sblob_idx:put(Idx, 25, a),
    ?assertError(badarg, sblob_idx:closest(Idx1, 2400)).

put_get_out_of_bounds_lower_test() ->
    Idx = sblob_idx:new(22),
    Idx1 = sblob_idx:put(Idx, 25, a),
    ?assertError(badarg, sblob_idx:closest(Idx1, 21)).

put_two_closest_test() ->
    Idx = sblob_idx:new(22),
    Idx1 = sblob_idx:put(Idx, 22, a),
    Idx2 = sblob_idx:put(Idx1, 24, b),
    {Key, Val} = sblob_idx:closest(Idx2, 25),
    ?assertEqual(Key, 24),
    ?assertEqual(Val, b).
