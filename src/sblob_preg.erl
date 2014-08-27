-module(sblob_preg).
-export([new/0, put/3, update/3, get/2, remove/2, to_each/2, foreach/2]).

new() -> gb_trees:empty().

put(Preg, Key, Val) -> gb_trees:insert(Key, Val, Preg).
get(Preg, Key) -> gb_trees:lookup(Key, Preg).
remove(Preg, Key) -> gb_trees:delete(Key, Preg).
update(Preg, Key, Val) -> gb_trees:update(Key, Val, Preg).

% iterates over all key values and updates value with value returned from Fun
to_each(Preg, Fun) ->
    Iter = gb_trees:iterator(Preg),
    to_each(Preg, Fun, Iter).

to_each(Preg, _Fun, nil) ->
    Preg;

to_each(Preg, Fun, Iter) ->
    {NewPreg, NewIter} = case gb_trees:next(Iter) of
                             none -> {Preg, nil};
                             {Key, Val, Iter1} ->
                                 NewVal = Fun(Key, Val),
                                 Preg1 = update(Preg, Key, NewVal),
                                 {Preg1, Iter1}
                         end,
    to_each(NewPreg, Fun, NewIter).

% iterates over all key values calling Fun for each pair, doesn't change the
% current value
foreach(Preg, Fun) ->
    Iter = gb_trees:iterator(Preg),
    foreach(Preg, Fun, Iter).

foreach(Preg, _Fun, nil) ->
    Preg;

foreach(Preg, Fun, Iter) ->
    NewIter = case gb_trees:next(Iter) of
                  none -> nil;
                  {Key, Val, Iter1} ->
                      Fun(Key, Val),
                      Iter1
              end,
    foreach(Preg, Fun, NewIter).

