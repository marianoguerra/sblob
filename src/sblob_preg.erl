-module(sblob_preg).
-export([new/0, put/3, update/3, get/2, remove/2, foreach/2]).

new() -> ets:new(preg, [protected, ordered_set]).

put(Preg, Key, Val) ->
    ets:insert(Preg, {Key, Val}),
    Preg.

get(Preg, Key) ->
    case ets:lookup(Preg, Key) of
        [{Key, Val}] -> {value, Val};
        [] -> none
    end.

remove(Preg, Key) ->
    ets:delete(Preg, Key),
    Preg.

update(Preg, Key, Val) -> put(Preg, Key, Val).

% iterates over all key values calling Fun for each pair, doesn't change the
% current value
foreach(Preg, Fun) ->
    Key = ets:first(Preg),
    foreach(Preg, Fun, Key).

foreach(Preg, _Fun, '$end_of_table') ->
    Preg;

foreach(Preg, Fun, Key) ->
    {value, Val} = get(Preg, Key),
    Fun(Key, Val),
    NextKey = ets:next(Preg, Key),
    foreach(Preg, Fun, NextKey).

