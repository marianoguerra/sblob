-module(sblob_tests).
-include_lib("eunit/include/eunit.hrl").
-include("sblob.hrl").

usage_test_() ->
    ?debugMsg("starting sblob usage tests"),
    file_handle_cache:start_link(),
    lager:start(),
    %lager:set_loglevel(lager_console_backend, debug),
    {foreach,
     fun usage_start/0,
     fun usage_stop/1,
     [fun do_nothing/1,
      fun new_sblob_has_zero_size/1,
      fun open_write_close_open_has_correct_size/1,
      fun write_one/1,
      fun write_one_read_one/1,

      fun no_write_stats/1,
      fun write_stats/1,
      fun write_reopen_stats/1,

      fun no_write_read_one/1,
      fun no_write_read_one_1/1,
      fun no_write_read_many/1,
      fun no_write_read_many_1/1,

      fun write_close_write_read_all/1,

      fun write_one_read_last/1,
      fun write_two_read_first/1,
      fun write_two_read_last/1,
      fun write_two_read_first_and_last/1,

      fun write_4_read_after_last_returns_empty/1,
      fun write_4_read_before_first_returns_4/1,

      fun write_4_read_all/1,
      fun write_4_read_first_2/1,
      fun write_4_read_middle_2/1,
      fun write_4_read_last_2/1,
      fun write_4_read_past_end/1,
      fun write_4_read_out_of_bounds_end/1,

      fun write_4_close_read_all/1,
      fun write_4_close_read_first_2/1,
      fun write_4_close_read_middle_2/1,
      fun write_4_close_read_last_2/1,
      fun write_4_close_read_past_end/1,
      fun write_4_close_read_out_of_bounds_end/1,

      fun write_4_close_read_all_raw/1,
      fun write_4_close_read_first_2_raw/1,
      fun write_4_close_read_middle_2_raw/1,
      fun write_4_close_read_last_2_raw/1
     ]}.

reopen(#sblob{path=Path, name=Name}=Sblob) ->
    sblob:close(Sblob),
    sblob:open(Path, Name, []).

gen_stream_info() ->
    Path = "bucket",
    Name = io_lib:format("stream~p", [sblob_util:now()]),
    {Path, Name}.

usage_start() ->
    %?debugMsg("S---------------------------------------------------------"),
    {Path, Name} = gen_stream_info(),
    sblob:open(Path, Name, []).

usage_stop(Sblob) ->
    sblob:delete(Sblob).
    %?debugMsg("E---------------------------------------------------------").

do_nothing(_Sblob) -> [].

new_sblob_has_zero_size(Sblob) ->
    ?_assertEqual(Sblob#sblob.size, 0).

open_write_close_open_has_correct_size(Sblob) ->
    Data = <<"hola">>,
    {Sblob1, Entry} = sblob:put(Sblob, Data),
    NewSblob = reopen(Sblob1),
    ?_assertEqual(Entry#sblob_entry.size, NewSblob#sblob.size).

write_one(#sblob{seqnum=SeqNum}=Sblob) ->
    Data = <<"hello sblob">>,
    {#sblob{seqnum=NewSeqNum}, #sblob_entry{seqnum=EntrySeqNum}} = sblob:put(Sblob, Data),
    [?_assertEqual(SeqNum + 1, NewSeqNum),
     ?_assertEqual(EntrySeqNum, NewSeqNum)].

no_write_read_one(Sblob) ->
    {_NewSblob, Result} = sblob:get(Sblob, 1),
    ?_assertEqual(Result, notfound).

no_write_read_one_1(Sblob) ->
    {_NewSblob, Result} = sblob:get(Sblob, 10),
    ?_assertEqual(Result, notfound).

no_write_read_many(Sblob) ->
    {_NewSblob, Result} = sblob:get(Sblob, 1, 10),
    ?_assertEqual(Result, []).

write_close_write_read_all(Sblob) ->
    Data1 = <<"a">>,
    Data2 = <<"b">>,
    {Sblob1, Entry1} = sblob:put(Sblob, Data1),
    Sblob2 = reopen(Sblob1),
    {Sblob3, Entry2} = sblob:put(Sblob2, Data2),
    {_Sblob4, Entries} = sblob:get(Sblob3, 1, 2),
    ?assertEqual(2, length(Entries)),
    [E1, E2] = Entries,
    [assert_entry(E1, <<"a">>, 1),
     assert_entry(E2, <<"b">>, 2),
     ?_assertEqual(E1, Entry1),
     ?_assertEqual(E2, Entry2)].

no_write_read_many_1(Sblob) ->
    {_NewSblob, Result} = sblob:get(Sblob, 10, 8),
    ?_assertEqual(Result, []).

write_4_read_all(Sblob) ->
    Sblob1 = write_many(Sblob, "asd ", 4),
    {_Sblob2, Result} = sblob:get(Sblob1, 1, 4),
    [?_assertEqual(length(Result), 4)].

write_4_read_after_last_returns_empty(Sblob) ->
    Sblob1 = write_many(Sblob, "asd ", 4),
    {Sblob2, Result} = sblob:get(Sblob1, 5, 4),
    {_Sblob3, Result1} = sblob:get(Sblob2, 4, 4),
    [?_assertEqual(0, length(Result)),
    ?_assertEqual(1, length(Result1))].

write_4_read_before_first_returns_4(Sblob) ->
    Sblob1 = write_many(Sblob, "asd ", 4),
    {Sblob2, Result} = sblob:get(Sblob1, 0, 4),
    {_Sblob3, Result1} = sblob:get(Sblob2, -10, 4),
    [?_assertEqual(4, length(Result)),
    ?_assertEqual(4, length(Result1))].

assert_entry(#sblob_entry{data=Data, seqnum=SeqNum, len=Len}, EData, ESeqNum) ->
    [?_assertEqual(Data, EData),
     ?_assertEqual(SeqNum, ESeqNum),
     ?_assertEqual(Len, size(Data))].

write_4_read_first_2(Sblob) ->
    Sblob1 = write_many(Sblob, "asd ", 4),
    {_Sblob2, Entries} = sblob:get(Sblob1, 1, 2),
    ?assertEqual(2, length(Entries)),
    [E1, E2] = Entries,
    [assert_entry(E1, <<"asd 0">>, 1),
     assert_entry(E2, <<"asd 1">>, 2)].

write_4_read_middle_2(Sblob) ->
    Sblob1 = write_many(Sblob, "asd ", 4),
    {_Sblob2, Entries} = sblob:get(Sblob1, 2, 2),
    ?assertEqual(2, length(Entries)),
    [E1, E2] = Entries,
    [assert_entry(E1, <<"asd 1">>, 2),
     assert_entry(E2, <<"asd 2">>, 3)].

write_4_read_last_2(Sblob) ->
    Sblob1 = write_many(Sblob, "asd ", 4),
    {_Sblob2, Entries} = sblob:get(Sblob1, 3, 2),
    ?assertEqual(2, length(Entries)),
    [E1, E2] = Entries,
    [assert_entry(E1, <<"asd 2">>, 3),
     assert_entry(E2, <<"asd 3">>, 4)].

write_4_read_past_end(Sblob) ->
    Sblob1 = write_many(Sblob, "asd ", 4),
    {_Sblob2, Entries} = sblob:get(Sblob1, 3, 20),
    ?assertEqual(2, length(Entries)),
    [E1, E2] = Entries,
    [assert_entry(E1, <<"asd 2">>, 3),
     assert_entry(E2, <<"asd 3">>, 4)].

write_4_read_out_of_bounds_end(Sblob) ->
    Sblob1 = write_many(Sblob, "asd ", 4),
    {_Sblob2, Result} = sblob:get(Sblob1, 5, 20),
    ?_assertEqual(Result, []).

write_4_close_read_all(Sblob) ->
    Sblob1 = reopen(write_many(Sblob, "asd ", 4)),
    {_Sblob2, Result} = sblob:get(Sblob1, 1, 4),
    [?_assertEqual(length(Result), 4)].

write_4_close_read_first_2(Sblob) ->
    Sblob1 = reopen(write_many(Sblob, "asd ", 4)),
    {_Sblob2, Entries} = sblob:get(Sblob1, 1, 2),
    ?assertEqual(2, length(Entries)),
    [E1, E2] = Entries,
    [assert_entry(E1, <<"asd 0">>, 1),
     assert_entry(E2, <<"asd 1">>, 2)].

write_4_close_read_middle_2(Sblob) ->
    Sblob1 = reopen(write_many(Sblob, "asd ", 4)),
    {_Sblob2, Entries} = sblob:get(Sblob1, 2, 2),
    ?assertEqual(2, length(Entries)),
    [E1, E2] = Entries,
    [assert_entry(E1, <<"asd 1">>, 2),
     assert_entry(E2, <<"asd 2">>, 3)].

write_4_close_read_last_2(Sblob) ->
    Sblob1 = reopen(write_many(Sblob, "asd ", 4)),
    {_Sblob2, Entries} = sblob:get(Sblob1, 3, 2),
    ?assertEqual(2, length(Entries)),
    [E1, E2] = Entries,
    [assert_entry(E1, <<"asd 2">>, 3),
     assert_entry(E2, <<"asd 3">>, 4)].

write_4_close_read_past_end(Sblob) ->
    Sblob1 = reopen(write_many(Sblob, "asd ", 4)),
    {_Sblob2, Entries} = sblob:get(Sblob1, 3, 20),
    ?assertEqual(2, length(Entries)),
    [E1, E2] = Entries,
    [assert_entry(E1, <<"asd 2">>, 3),
     assert_entry(E2, <<"asd 3">>, 4)].

write_4_close_read_out_of_bounds_end(Sblob) ->
    Sblob1 = reopen(write_many(Sblob, "asd ", 4)),
    {_Sblob2, Result} = sblob:get(Sblob1, 5, 20),
    ?_assertEqual(Result, []).

write_4_close_read_all_raw(#sblob{path=Path, name=ChunkName}=Sblob) ->
    Sblob1 = write_many(Sblob, "asd ", 4),
    _Sblob2 = sblob:close(Sblob1),
    Count = 4,
    SeqNum = 1,
    {[E1, E2, E3, E4], RFirstSN, RSeqNum, RCount} = sblob_util:seqread(Path, ChunkName, SeqNum, Count, []),
    [?_assertEqual(RSeqNum, 4),
     ?_assertEqual(RFirstSN, 1),
     ?_assertEqual(RCount, 4),
     assert_entry(E1, <<"asd 0">>, 1),
     assert_entry(E2, <<"asd 1">>, 2),
     assert_entry(E3, <<"asd 2">>, 3),
     assert_entry(E4, <<"asd 3">>, 4)].

write_4_close_read_first_2_raw(#sblob{path=Path, name=ChunkName}=Sblob) ->
    Sblob1 = write_many(Sblob, "asd ", 4),
    _Sblob2 = sblob:close(Sblob1),
    Count = 2,
    SeqNum = 1,
    {[E1, E2], RFirstSN, RSeqNum, RCount} = sblob_util:seqread(Path, ChunkName, SeqNum, Count, []),
    [?_assertEqual(RSeqNum, 2),
     ?_assertEqual(RFirstSN, 1),
     ?_assertEqual(RCount, 2),
     assert_entry(E1, <<"asd 0">>, 1),
     assert_entry(E2, <<"asd 1">>, 2)].

write_4_close_read_middle_2_raw(#sblob{path=Path, name=ChunkName}=Sblob) ->
    Sblob1 = write_many(Sblob, "asd ", 4),
    _Sblob2 = sblob:close(Sblob1),
    Count = 2,
    SeqNum = 2,
    {[E2, E3], RFirstSN, RSeqNum, RCount} = sblob_util:seqread(Path, ChunkName, SeqNum, Count, []),
    [?_assertEqual(RSeqNum, 3),
     ?_assertEqual(RFirstSN, 1),
     ?_assertEqual(RCount, 2),
     assert_entry(E2, <<"asd 1">>, 2),
     assert_entry(E3, <<"asd 2">>, 3)].

write_4_close_read_last_2_raw(#sblob{path=Path, name=ChunkName}=Sblob) ->
    Sblob1 = write_many(Sblob, "asd ", 4),
    _Sblob2 = sblob:close(Sblob1),
    Count = 4,
    SeqNum = 3,
    {[E3, E4], RFirstSN, RSeqNum, RCount} = sblob_util:seqread(Path, ChunkName, SeqNum, Count, []),
    [?_assertEqual(RSeqNum, 4),
     ?_assertEqual(RFirstSN, 1),
     ?_assertEqual(RCount, 2),
     assert_entry(E3, <<"asd 2">>, 3),
     assert_entry(E4, <<"asd 3">>, 4)].

no_write_stats(Sblob) ->
    Stats = sblob:stats(Sblob),
    ?_assertEqual(Stats, #sblob_stats{first_sn=0, last_sn=0, count=0, size=0}).

write_stats(Sblob) ->
    {Sblob1, _Entry} = sblob:put(Sblob, <<"lala">>),
    Stats = sblob:stats(Sblob1),
    ?_assertEqual(Stats, #sblob_stats{first_sn=0, last_sn=1, count=1, size=28}).

write_reopen_stats(Sblob) ->
    {Sblob1, _Entry} = sblob:put(Sblob, <<"lala">>),
    Stats = sblob:stats(reopen(Sblob1)),
    ?_assertEqual(Stats, #sblob_stats{first_sn=0, last_sn=1, count=1, size=28}).

write_one_read_one(Sblob) ->
    Data = <<"hello sblob!">>,
    {#sblob{seqnum=NewSeqNum}=Sblob1,
     #sblob_entry{seqnum=WSn, timestamp=WTs, data=WData}} = sblob:put(Sblob, Data),

    {_NewSblob,
     #sblob_entry{timestamp=RTs, seqnum=RSn, len=RLen, data=RData}} = sblob:get(Sblob1, WSn),

    [?_assertEqual(RTs, WTs),
     ?_assertEqual(RSn, WSn),
     ?_assertEqual(RSn, NewSeqNum),
     ?_assertEqual(RLen, size(Data)),
     ?_assertEqual(RData, WData)].

write_one_read_last(Sblob) ->
    Data = <<"hello sblob!">>,
    {#sblob{seqnum=NewSeqNum}=Sblob1,
     #sblob_entry{seqnum=WSn, timestamp=WTs, data=WData}} = sblob:put(Sblob, Data),

    {_NewSblob,
     #sblob_entry{timestamp=RTs, seqnum=RSn, len=RLen, data=RData}} = sblob:get(Sblob1, 1),

    [?_assertEqual(RTs, WTs),
     ?_assertEqual(RSn, WSn),
     ?_assertEqual(RSn, NewSeqNum),
     ?_assertEqual(RLen, size(Data)),
     ?_assertEqual(RData, WData)].

write(Sblob, Data) ->
    Put = sblob:put(Sblob, Data),
    {NewSblob, Entry} = Put,
    {Data, Put, NewSblob, Entry}.

write_many(Sblob, Base, Count) ->
    write_many(Sblob, Base, Count, 0).

write_many(Sblob, _Base, 0, _I) ->
    Sblob;

write_many(Sblob, Base, Count, I) ->
    DataStr = io_lib:format("~s~p", [Base, I]),
    Data = list_to_binary(DataStr),
    {_, _, NewSblob, _} = write(Sblob, Data),
    write_many(NewSblob, Base, Count - 1, I + 1).

write_first(Sblob) ->
    Data = <<"hello sblob head!">>,
    write(Sblob, Data).

write_second(Sblob) ->
    Data = <<"hello sblob tail!">>,
    write(Sblob, Data).

write_two(Sblob) ->
    {HData, _HPut, HSblob, HEntry} = write_first(Sblob),
    {TData, _TPut, TSblob, TEntry} = write_second(HSblob),
    {TSblob, HEntry, HData, TEntry, TData}.

write_two_read_first(Sblob) ->
    {NewSblob, HEntry, HData, _TEntry, _TData} = write_two(Sblob),

    #sblob_entry{seqnum=HWSn, timestamp=HWTs, data=HWData} = HEntry,
    {_NewSblob1, RHEntry} = sblob:get(NewSblob, 1),
    #sblob_entry{timestamp=RHTs, seqnum=RHSn, len=RHLen, data=RHData} = RHEntry,

    % read head asserts
    [?_assertEqual(RHTs, HWTs),
     ?_assertEqual(RHSn, HWSn),
     ?_assertEqual(RHSn, 1),
     ?_assertEqual(RHLen, size(HData)),
     ?_assertEqual(RHData, HWData)].

write_two_read_last(Sblob) ->
    {NewSblob, _HEntry, _HData, TEntry, TData} = write_two(Sblob),

    #sblob_entry{seqnum=TWSn, timestamp=TWTs, data=TWData} = TEntry,
    {_RTSblob, RTEntry} = sblob:get(NewSblob, 2),

    #sblob_entry{timestamp=RTTs, seqnum=RTSn, len=RTLen, data=RTData} = RTEntry,

    % read tail asserts
    [?_assertEqual(RTTs, TWTs),
     ?_assertEqual(RTSn, TWSn),
     ?_assertEqual(RTSn, 2),
     ?_assertEqual(RTLen, size(TData)),
     ?_assertEqual(RTData, TWData)].

write_two_read_first_and_last(Sblob) ->
    {NewSblob, HEntry, HData, TEntry, TData} = write_two(Sblob),

    #sblob_entry{seqnum=HWSn, timestamp=HWTs, data=HWData} = HEntry,
    #sblob_entry{seqnum=TWSn, timestamp=TWTs, data=TWData} = TEntry,

    {NewSblob1, RHEntry} = sblob:get(NewSblob, 1),
    #sblob_entry{timestamp=RHTs, seqnum=RHSn, len=RHLen, data=RHData} = RHEntry,

    % read tail
    {_RTSblob, RTEntry} = sblob:get(NewSblob1, 2),

    #sblob_entry{timestamp=RTTs, seqnum=RTSn, len=RTLen, data=RTData} = RTEntry,

    % read head asserts
    [?_assertEqual(RHTs, HWTs),
     ?_assertEqual(RHSn, HWSn),
     ?_assertEqual(RHSn, 1),
     ?_assertEqual(RHLen, size(HData)),
     ?_assertEqual(RHData, HWData),

    % read tail asserts
     ?_assertEqual(RTTs, TWTs),
     ?_assertEqual(RTSn, TWSn),
     ?_assertEqual(RTSn, 2),
     ?_assertEqual(RTLen, size(TData)),
     ?_assertEqual(RTData, TWData)].

open_test() ->
    {Path, Name} = gen_stream_info(),
    AbsPath = filename:absname(Path),
    FullPath = filename:join([AbsPath, Name]),
    Sblob = sblob:open(Path, Name, []),
    true = filelib:is_file(FullPath),
    ?assertEqual(AbsPath, Sblob#sblob.path),
    ?assertEqual(FullPath, Sblob#sblob.fullpath),
    ?assertEqual(Name, Sblob#sblob.name),
    ?assertEqual(0, Sblob#sblob.seqnum),
    ?assertEqual(#sblob_cfg{}, Sblob#sblob.config).

write_max_items_test() ->
    {Path, Name} = gen_stream_info(),
    Sblob = sblob:open(Path, Name, [{max_items, 10}]),
    _Sblob1 = write_many(Sblob, "asd ", 10).

write_close_open_write_max_items_test() ->
    {Path, Name} = gen_stream_info(),
    Sblob = sblob:open(Path, Name, [{max_items, 10}]),
    Sblob1 = write_many(Sblob, "asd ", 5),
    sblob:close(Sblob1),
    Sblob2 = sblob:open(Path, Name, [{max_items, 10}]),
    _Sblob3 = write_many(Sblob2, "asd ", 5).

to_from_binary_test() ->
    Timestamp = 123456,
    SeqNum = 432109,
    Data = <<"hi there">>,
    Bin = sblob_util:to_binary(Timestamp, SeqNum, Data),
    #sblob_entry{timestamp=T, seqnum=S, data=D} = sblob_util:from_binary(Bin),
    ?assertEqual(Timestamp, T),
    ?assertEqual(SeqNum, S),
    ?assertEqual(Data, D).

parse_empty_config_test() ->
    Cfg = sblob_util:parse_config([]),
    ?assertEqual(Cfg#sblob_cfg.max_items, 4096),
    ?assertEqual(Cfg#sblob_cfg.base_seqnum, 0),
    ?assertEqual(Cfg#sblob_cfg.read_ahead, 65536).

parse_config_test() ->
    Cfg = sblob_util:parse_config([{max_items, 1}, {base_seqnum, 50},
                                   {read_ahead, 0}]),
    ?assertEqual(Cfg#sblob_cfg.max_items, 1),
    ?assertEqual(Cfg#sblob_cfg.base_seqnum, 50),
    ?assertEqual(Cfg#sblob_cfg.read_ahead, 0).

test_recover(BaseName, Name, Uid) ->
    BaseDir = filename:absname("../test/data/broken"),
    AbsBaseName = filename:join(BaseDir, BaseName),
    AbsName = filename:join(BaseDir, Name),
    BrokenName = "broken." ++ Name ++ "." ++ Uid,

    {ok, _} = file:copy(AbsBaseName, AbsName),

    RecoverPath = filename:join(BaseDir, BrokenName),
    Sblob = sblob:open(BaseDir, Name, [{uid, Uid}]),
    _Sblob1 = sblob:close(Sblob),

    RecoverExists = filelib:is_regular(RecoverPath),

    file:delete(AbsName),
    file:delete(RecoverPath),

    ?assertEqual(RecoverExists, true).

recover_broken_sblob_test() ->
    test_recover("init-alarms", "broken1", "uid1").

recover_broken_entry_zeros_test() ->
    test_recover("zeros", "broken2", "uid2").

recover_broken_short_header_test() ->
    test_recover("shortheader", "broken3", "uid3").

fold_fun(#sblob_entry{size=Size}, CurSize) ->
    NewSize = CurSize + Size,
    {continue, NewSize}.

test_fold_truncated_test() ->
    R = sblob_util:fold("../test/data/broken", "truncated", [], fun fold_fun/2,
                        0),
    ?assertEqual(R, {error, {short_read, {expected, 721, got, 710}}, 1482}).

test_fold_badlen_test() ->
    R = sblob_util:fold("../test/data/broken", "badlen", [], fun fold_fun/2,
                        0),
    ?assertEqual(R, {error,{corrupt_len_field,{717,24842}},1482}).
