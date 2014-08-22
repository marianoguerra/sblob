-module(nsort).
% http://erlangcentral.org/wiki/index.php/String_Sorting_%28Natural%29

-export([sort/1]).

%%%----------------------------------------------------------------------------
%%% @spec sort(List) -> list()
%%%     List = list()
%%%
%%% @doc Sorts a list of strings with integers in a natural way.
%%% @end
%%%----------------------------------------------------------------------------
sort(List) ->
    lists:map(fun({_, K}) -> K end,
          lists:keysort(1, lists:map(fun alphanum_key/1, List))).

alphanum_key(Term) when is_list(Term) ->
    {alphanum_key(Term, term, []), Term};
alphanum_key(Term) ->
    {Term, Term}.


alphanum_key([], integer, [H|List]) ->
    lists:reverse([list_to_integer(H)|List]);
alphanum_key([], term, List) ->
    lists:reverse(List);
alphanum_key([C|Term], _Type, [])
  when is_integer(C), C >= 48, C =< 57 ->
    alphanum_key(Term, integer, [[C]]);
alphanum_key([C|Term], _Type, []) ->
    alphanum_key(Term, term, [[C]]);
alphanum_key([C|Term], integer, [H|List])
  when is_integer(C), C >= 48, C =< 57 ->
    alphanum_key(Term, integer, [H ++ [C]|List]);
alphanum_key([C|Term], integer, [H|List]) ->
    alphanum_key(Term, term, [[C]] ++ [list_to_integer(H)|List]);
alphanum_key([C|Term], term, [H|List])
  when is_integer(C), C >= 48, C =< 57 ->
    alphanum_key(Term, integer, [[C]] ++ [H|List]);
alphanum_key([C|Term], term, [H|List]) ->
    alphanum_key(Term, term, [H ++ [C]|List]).

