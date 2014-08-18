-module(sblob_tests).
-include_lib("eunit/include/eunit.hrl").
-include("sblob.hrl").

usage_test_() ->
    {foreach,
     fun usage_start/0,
     fun usage_stop/1,
     [fun do_nothing/1,
      fun write_one/1,
      fun write_one_read_one/1,
      fun write_one_read_last/1,
      fun write_two_read_first/1,
      fun write_two_read_last/1,
      fun write_two_read_first_and_last/1
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
     ?_assertEqual(EntrySeqNum, NewSeqNum - 1)].

write_one_read_one(Sblob) ->
    Data = <<"hello sblob!">>,
    {#sblob{seqnum=NewSeqNum},
     #sblob_entry{seqnum=WSn, timestamp=WTs, data=WData}} = sblob:put(Sblob, Data),

    {_NewSblob,
     #sblob_entry{timestamp=RTs, seqnum=RSn, len=RLen, data=RData}} = sblob:get(Sblob, WSn),

    [?_assertEqual(RTs, WTs),
     ?_assertEqual(RSn, WSn),
     ?_assertEqual(RSn, NewSeqNum - 1),
     ?_assertEqual(RLen, size(Data)),
     ?_assertEqual(RData, WData)].

write_one_read_last(Sblob) ->
    Data = <<"hello sblob!">>,
    {#sblob{seqnum=NewSeqNum},
     #sblob_entry{seqnum=WSn, timestamp=WTs, data=WData}} = sblob:put(Sblob, Data),

    {_NewSblob,
     #sblob_entry{timestamp=RTs, seqnum=RSn, len=RLen, data=RData}} = sblob:get_last_in_current(Sblob),

    [?_assertEqual(RTs, WTs),
     ?_assertEqual(RSn, WSn),
     ?_assertEqual(RSn, NewSeqNum - 1),
     ?_assertEqual(RLen, size(Data)),
     ?_assertEqual(RData, WData)].

write(Sblob, Data) ->
    Put = sblob:put(Sblob, Data),
    {NewSblob, Entry} = Put,
    {Data, Put, NewSblob, Entry}.

write_first(Sblob) ->
    Data = <<"hello sblob head!">>,
    write(Sblob, Data).

write_second(Sblob) ->
    Data = <<"hello sblob tail!">>,
    write(Sblob, Data).

read_first(Sblob) ->
    sblob:get_first_in_current(Sblob).

write_two(Sblob) ->
    {HData, _HPut, HSblob, HEntry} = write_first(Sblob),
    {TData, _TPut, TSblob, TEntry} = write_second(HSblob),
    {TSblob, HEntry, HData, TEntry, TData}.

write_two_read_first(Sblob) ->
    {NewSblob, HEntry, HData, _TEntry, _TData} = write_two(Sblob),

    #sblob_entry{seqnum=HWSn, timestamp=HWTs, data=HWData} = HEntry,
    {_NewSblob1, RHEntry} = read_first(NewSblob),
    #sblob_entry{timestamp=RHTs, seqnum=RHSn, len=RHLen, data=RHData} = RHEntry,

    % read head asserts
    [?_assertEqual(RHTs, HWTs),
     ?_assertEqual(RHSn, HWSn),
     ?_assertEqual(RHSn, 0),
     ?_assertEqual(RHLen, size(HData)),
     ?_assertEqual(RHData, HWData)].

write_two_read_last(Sblob) ->
    {NewSblob, _HEntry, _HData, TEntry, TData} = write_two(Sblob),

    #sblob_entry{seqnum=TWSn, timestamp=TWTs, data=TWData} = TEntry,
    {_RTSblob, RTEntry} = sblob:get_last_in_current(NewSblob),
    
    #sblob_entry{timestamp=RTTs, seqnum=RTSn, len=RTLen, data=RTData} = RTEntry,

    % read tail asserts
    [?_assertEqual(RTTs, TWTs),
     ?_assertEqual(RTSn, TWSn),
     ?_assertEqual(RTSn, 1),
     ?_assertEqual(RTLen, size(TData)),
     ?_assertEqual(RTData, TWData)].

write_two_read_first_and_last(Sblob) ->
    {NewSblob, HEntry, HData, TEntry, TData} = write_two(Sblob),

    #sblob_entry{seqnum=HWSn, timestamp=HWTs, data=HWData} = HEntry,
    #sblob_entry{seqnum=TWSn, timestamp=TWTs, data=TWData} = TEntry,
   
    {NewSblob1, RHEntry} = read_first(NewSblob),
    #sblob_entry{timestamp=RHTs, seqnum=RHSn, len=RHLen, data=RHData} = RHEntry,
            
    % read tail
    {_RTSblob, RTEntry} = sblob:get_last_in_current(NewSblob1),
    
    #sblob_entry{timestamp=RTTs, seqnum=RTSn, len=RTLen, data=RTData} = RTEntry,

    % read head asserts
    [?_assertEqual(RHTs, HWTs),
     ?_assertEqual(RHSn, HWSn),
     ?_assertEqual(RHSn, 0),
     ?_assertEqual(RHLen, size(HData)),
     ?_assertEqual(RHData, HWData),

    % read tail asserts
     ?_assertEqual(RTTs, TWTs),
     ?_assertEqual(RTSn, TWSn),
     ?_assertEqual(RTSn, 1),
     ?_assertEqual(RTLen, size(TData)),
     ?_assertEqual(RTData, TWData)].

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
