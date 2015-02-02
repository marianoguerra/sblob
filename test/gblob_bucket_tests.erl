-module(gblob_bucket_tests).
-include_lib("eunit/include/eunit.hrl").

-include("sblob.hrl").

usage_test_() ->
    file_handle_cache:start_link(),
    ?debugMsg("starting gblob bucket usage tests"),
    {foreach,
     fun usage_start/0,
     fun usage_stop/1,
     [fun do_nothing/1,
      fun write_expect_last_seqnum/1,
      fun write_buckets_get_size/1,
      fun write_buckets_evict_get_size/1,
      fun write_buckets_evict_write_get_size/1
     ]}.

usage_start() ->
    new_gblob_bucket().

usage_stop(Bucket) ->
    gblob_bucket:stop(Bucket).

new_gblob_bucket() ->
    Path = io_lib:format("bucket-~p", [sblob_util:now()]),
    {ok, Bucket} = gblob_bucket:start_link(Path, [{max_items, 10}], []),
    Bucket.

num_to_data(Num) ->
    Str = io_lib:format("item ~p", [Num]),
    list_to_binary(Str).

write_many(Bucket, Id, Count) ->
    lists:foreach(fun (I) ->
                       Data = num_to_data(I),
                       gblob_bucket:put(Bucket, Id, Data)
               end, lists:seq(0, Count - 1)).

do_nothing(_Bucket) -> [].

write_expect_last_seqnum(Bucket) ->
    Id = <<"mystream">>,
    Timestamp = 42,
    Data = <<"hi there">>,

    Self = self(),

    Cb3 = fun(Error) -> Self ! Error end,


    Cb1 = fun (#sblob_entry{seqnum=LastSeqnum}) ->
                  ?assertEqual(1, LastSeqnum),
                  Cb2 = fun (#sblob_entry{seqnum=LastSeqnum1}) ->
                                ?assertEqual(2, LastSeqnum1),
                                gblob_bucket:put_cb(Bucket, Id, Timestamp, Data,
                                                 LastSeqnum, Cb3)
                        end,

                  gblob_bucket:put_cb(Bucket, Id, Timestamp, Data, LastSeqnum, Cb2)
          end,

    gblob_bucket:put_cb(Bucket, Id, Timestamp, Data, Cb1),

    Error = receive X -> X after 1000 -> timeout end,
    [?_assertEqual({error, {conflict, {seqnum, 2, expected, 1}}}, Error)].

write_buckets_get_size(Bucket) ->
    write_many(Bucket, <<"b1">>, 12),
    write_many(Bucket, <<"b2">>, 12),
    write_many(Bucket, <<"b3">>, 6),
    write_many(Bucket, <<"b3">>, 6),
    write_many(Bucket, <<"b4">>, 12),
    write_many(Bucket, <<"b5">>, 10),

    {TotalSize, BucketSizes} = gblob_bucket:size(Bucket),
    B1 = proplists:get_value(<<"b1">>, BucketSizes),
    B2 = proplists:get_value(<<"b2">>, BucketSizes),
    B3 = proplists:get_value(<<"b3">>, BucketSizes),
    B4 = proplists:get_value(<<"b4">>, BucketSizes),
    B5 = proplists:get_value(<<"b5">>, BucketSizes),

    % just check the numbers they return I didn't calculate the sizes hand
    [?_assertEqual(1746, TotalSize),
     ?_assertEqual(362, B1),
     ?_assertEqual(362, B2),
     ?_assertEqual(360, B3), % goes from 0 to 5 two times
     ?_assertEqual(362, B4),
     ?_assertEqual(300, B5)].

write_buckets_evict_get_size(Bucket) ->
    write_many(Bucket, <<"b1">>, 12),
    write_many(Bucket, <<"b2">>, 12),
    write_many(Bucket, <<"b3">>, 6),
    write_many(Bucket, <<"b3">>, 6),
    write_many(Bucket, <<"b4">>, 12),
    write_many(Bucket, <<"b5">>, 10),

    gblob_bucket:truncate_percentage(Bucket, 0.5),
    {TotalSize, BucketSizes} = gblob_bucket:size(Bucket),

    B1 = proplists:get_value(<<"b1">>, BucketSizes),
    B2 = proplists:get_value(<<"b2">>, BucketSizes),
    B3 = proplists:get_value(<<"b3">>, BucketSizes),
    B4 = proplists:get_value(<<"b4">>, BucketSizes),
    B5 = proplists:get_value(<<"b5">>, BucketSizes),

    % just check the numbers they return I didn't calculate the sizes hand
    [?_assertEqual(546, TotalSize),
     ?_assertEqual(62, B1),
     ?_assertEqual(62, B2),
     ?_assertEqual(60, B3), % goes from 0 to 5 two times
     ?_assertEqual(62, B4),
     ?_assertEqual(300, B5)].

write_buckets_evict_write_get_size(Bucket) ->
    write_many(Bucket, <<"b1">>, 12),
    write_many(Bucket, <<"b2">>, 12),
    write_many(Bucket, <<"b3">>, 6),
    write_many(Bucket, <<"b3">>, 6),
    write_many(Bucket, <<"b4">>, 12),
    write_many(Bucket, <<"b5">>, 10),

    gblob_bucket:truncate_percentage(Bucket, 0),

    write_many(Bucket, <<"b1">>, 12),
    write_many(Bucket, <<"b2">>, 12),
    write_many(Bucket, <<"b3">>, 6),
    write_many(Bucket, <<"b3">>, 6),
    write_many(Bucket, <<"b4">>, 12),
    write_many(Bucket, <<"b5">>, 10),

    {TotalSize, BucketSizes} = gblob_bucket:size(Bucket),

    B1 = proplists:get_value(<<"b1">>, BucketSizes),
    B2 = proplists:get_value(<<"b2">>, BucketSizes),
    B3 = proplists:get_value(<<"b3">>, BucketSizes),
    B4 = proplists:get_value(<<"b4">>, BucketSizes),
    B5 = proplists:get_value(<<"b5">>, BucketSizes),

    [?_assertEqual(2292, TotalSize),
     ?_assertEqual(424, B1),
     ?_assertEqual(424, B2),
     ?_assertEqual(420, B3), % goes from 0 to 5 two times
     ?_assertEqual(424, B4),
     ?_assertEqual(600, B5)].

