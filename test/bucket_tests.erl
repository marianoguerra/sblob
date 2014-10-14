-module(bucket_tests).
-include_lib("eunit/include/eunit.hrl").
-include("sblob.hrl").
-include("gblob.hrl").

usage_test_() ->
    file_handle_cache:start_link(),
    ?debugMsg("starting bucket usage tests"),
    {foreach,
     fun usage_start/0,
     fun usage_stop/1,
     [fun do_nothing/1,
      fun put_1_read_1/1,
      fun put_on_2_buckets_read_back_each/1,
      fun put_100_read_100/1,
      fun put_on_2_buckets_in_2_parts_read_back_each/1
     ]}.

usage_start() ->
    Path = io_lib:format("bucket-~p", [sblob_util:now()]),
    {ok, Bucket} = gblob_bucket:start_link(Path, [{max_items, 10}], []),
    Bucket.

usage_stop(Bucket) ->
    gblob_bucket:stop(Bucket).

do_nothing(_Bucket) -> [].

put_1_read_1(Bucket) ->
    WEntry = gblob_bucket:put(Bucket, <<"s1">>, <<"hi!">>),
    REntry = gblob_bucket:get(Bucket, <<"s1">>, 1),
    [?_assertEqual(REntry, WEntry)].

put_on_2_buckets_read_back_each(Bucket) ->
    WEntry1 = gblob_bucket:put(Bucket, <<"s1">>, <<"hi!">>),
    REntry1 = gblob_bucket:get(Bucket, <<"s1">>, 1),
    WEntry2 = gblob_bucket:put(Bucket, <<"s2">>, <<"hi there!">>),
    REntry2 = gblob_bucket:get(Bucket, <<"s2">>, 1),
    [?_assertEqual(REntry1, WEntry1),
     ?_assertEqual(REntry2, WEntry2)].

put_100_read_100(Bucket) ->
    write_N_read_N(Bucket, <<"s3">>, 100, 1, 100).

put_on_2_buckets_in_2_parts_read_back_each(Bucket) ->
    write_many(Bucket, <<"s1">>, 50),
    write_many(Bucket, <<"s2">>, 50),
    write_many(Bucket, <<"s1">>, 50, 50),
    write_many(Bucket, <<"s2">>, 50, 50),

    read_N(Bucket, <<"s1">>, 1, 100) ++ read_N(Bucket, <<"s2">>, 1, 100).

num_to_data(Num) ->
    Str = io_lib:format("item ~p", [Num]),
    list_to_binary(Str).

write_many(Bucket, Id, Count) ->
    write_many(Bucket, Id, Count, 0).

write_many(Bucket, Id, Count, Offset) ->
    lists:foreach(fun (I) ->
                       Data = num_to_data(I),
                       gblob_bucket:put(Bucket, Id, Data)
               end, lists:seq(Offset, Offset + Count - 1)).

read_N(Bucket, Id, StartSN, ReadCount) ->
    ?debugFmt("read ~p start at ~p~n", [ReadCount, StartSN]),
    Result = gblob_bucket:get(Bucket, Id, StartSN, ReadCount),
    Indexes = lists:seq(StartSN - 1, StartSN + ReadCount - 2),
    Items = lists:zip(Indexes, Result),
    lists:map(fun ({I, Entity}) ->
                      Data = num_to_data(I),
                      assert_entry(Entity, Data, I + 1)
              end, Items).

write_N_read_N(Bucket, Id, WriteCount, StartSN, ReadCount) ->
    write_many(Bucket, Id, WriteCount),
    read_N(Bucket, Id, StartSN, ReadCount).

assert_entry(#sblob_entry{data=Data, seqnum=SeqNum, len=Len}, EData, ESeqNum) ->
    [?_assertEqual(EData, Data),
     ?_assertEqual(ESeqNum, SeqNum),
     ?_assertEqual(size(Data), Len)].
