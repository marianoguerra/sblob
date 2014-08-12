-module(sblob_tests).
-include_lib("eunit/include/eunit.hrl").
-include("sblob.hrl").

usage_test_() ->
    {foreach,
     fun usage_start/0,
     fun usage_stop/1,
     [fun do_nothing/1,
      fun write_one/1,
      fun write_one_read_one/1
     ]}.

usage_start() ->
    Path = "bucket",
    Name = "stream",
    sblob:open(Path, Name, []).

usage_stop(Sblob) ->
    sblob:delete(Sblob).

do_nothing(_Sblob) -> [].

write_one(#sblob{seqnum=SeqNum}=Sblob) ->
    Data = <<"hello sblob">>,
    {#sblob{seqnum=NewSeqNum}, #sblob_entry{seqnum=EntrySeqNum}} = sblob:put(Sblob, Data),
    [?_assertEqual(SeqNum + 1, NewSeqNum),
     ?_assertEqual(EntrySeqNum, NewSeqNum)].

write_one_read_one(Sblob) ->
    Data = <<"hello sblob!">>,
    {#sblob{seqnum=NewSeqNum},
     #sblob_entry{seqnum=WSn, timestamp=WTs, data=WData}} = sblob:put(Sblob, Data),

    #sblob_entry{timestamp=RTs, seqnum=RSn, data=RData} = sblob:get(Sblob, WSn),

    [?_assertEqual(RTs, WTs),
     ?_assertEqual(RSn, WSn),
     ?_assertEqual(RSn, NewSeqNum),
     ?_assertEqual(RData, WData)].

open_test() ->
    Path = "foo",
    Name = "bar",
    AbsPath = filename:absname(Path),
    FullPath = filename:join([AbsPath, Name]),
    Sblob = sblob:open(Path, Name, []),
    true = filelib:is_dir(FullPath),
    #sblob{path=AbsPath, fullpath=FullPath, name=Name, seqnum=0,
           config=#sblob_cfg{}} = Sblob.

to_from_binary_test() ->
    Timestamp = 123456,
    SeqNum = 432109,
    Data = <<"hi there">>,
    Bin = sblob_util:to_binary(Timestamp, SeqNum, Data),
    #sblob_entry{timestamp=T, seqnum=S, data=D} = sblob_util:from_binary(Bin),
    ?assertEqual(Timestamp, T),
    ?assertEqual(SeqNum, S),
    ?assertEqual(Data, D).
