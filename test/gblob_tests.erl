-module(gblob_tests).
-include_lib("eunit/include/eunit.hrl").
-include("gblob.hrl").

usage_test_() ->
    {foreach,
     fun usage_start/0,
     fun usage_stop/1,
     [fun do_nothing/1,
      fun only_reopen/1,
      fun write_100/1
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

write_100(Gblob) ->
    lists:foldl(fun (I, GblobIn) ->
                       Str = io_lib:format("item ~p", [I]),
                       Data = list_to_binary(Str),
                       {GblobOut, _} = gblob:put(GblobIn, Data),
                       GblobOut
               end, Gblob, lists:seq(0, 100)),
    [].

open_test() ->
    Path = "gblob",
    AbsPath = filename:absname(Path),
    Gblob = gblob:open(Path, []),
    true = filelib:is_dir(Path),
    ?assertEqual(#gblob{path=AbsPath, config=#gblob_cfg{}}, Gblob).

get_blob_indexes_test() ->
    Indexes = gblob_util:get_blob_indexes_from_list(["sblob.5", "sblob.3", "sblob.4", "sblob.10"]),
    ?assertEqual(Indexes, [3, 4, 5, 10]).
