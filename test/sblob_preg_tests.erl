-module(sblob_preg_tests).
-include_lib("eunit/include/eunit.hrl").

new() -> sblob_preg:new().

put(Preg, Key, Val) ->
    sblob_preg:put(Preg, Key, Val).

get(Preg, Key) ->
    sblob_preg:get(Preg, Key).

getr(Preg, Val) ->
    sblob_preg:get_reverse(Preg, Val).

del(Preg, Key) ->
    sblob_preg:remove(Preg, Key).

delr(Preg, Val) ->
    sblob_preg:remove_reverse(Preg, Val).

put_get_reverse_test() ->
    Preg = new(),
    put(Preg, mykey, myval),
    Res = get(Preg, mykey),
    ResRev = getr(Preg, myval),
    ?assertEqual({value, myval}, Res),
    ?assertEqual({key, mykey}, ResRev).

put_get_reverse_del_test() ->
    Preg = new(),
    put(Preg, mykey, myval),
    Res = get(Preg, mykey),
    ResRev = getr(Preg, myval),
    del(Preg, mykey),
    Res1 = get(Preg, mykey),
    ResRev1 = getr(Preg, myval),
    ?assertEqual({value, myval}, Res),
    ?assertEqual({key, mykey}, ResRev),
    ?assertEqual(none, Res1),
    ?assertEqual(none, ResRev1).

put_get_reverse_del_reverse_test() ->
    Preg = new(),
    put(Preg, mykey, myval),
    put(Preg, otherkey, otherval),
    Res = get(Preg, mykey),
    ResRev = getr(Preg, myval),
    delr(Preg, myval),
    OtherRes = get(Preg, otherkey),
    OtherResRev = getr(Preg, otherval),
    Res1 = get(Preg, mykey),
    ResRev1 = getr(Preg, myval),
    ?assertEqual({value, myval}, Res),
    ?assertEqual({key, mykey}, ResRev),

    ?assertEqual({value, otherval}, OtherRes),
    ?assertEqual({key, otherkey}, OtherResRev),

    ?assertEqual(none, Res1),
    ?assertEqual(none, ResRev1).

foreach_test() ->
    Preg = new(),
    put(Preg, mykey, myval),
    put(Preg, otherkey, otherval),
    sblob_preg:foreach(Preg, fun (Key, Val) ->
                                     IsFirst = (Key == mykey andalso Val == myval),
                                     IsSecond = (Key == otherkey andalso Val == otherval),
                                     IsOne = IsFirst orelse IsSecond,
                                     true = IsOne
                             end).
