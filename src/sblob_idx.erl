-module(sblob_idx).
-export([new/1, new/2, put/3, closest/2, closest_value/2, expand/2]).

-record(sblob_idx, {base_key=0, data}).
-define(SBLOB_IDX_DEFAULT_INDEX_SIZE, 256).

new(BaseKey) ->
    new(BaseKey, ?SBLOB_IDX_DEFAULT_INDEX_SIZE).

new(BaseKey, MaxItems) ->
    Data = array:new(MaxItems),
    #sblob_idx{base_key=BaseKey, data=Data}.

put(#sblob_idx{data=Data, base_key=BaseKey}=Idx, Key, Val) ->
    I = Key - BaseKey,
    NewData = try array:set(I, Val, Data)
              catch error:badarg ->
                        lager:error("Error adding entry to index, base key ~p, i ~p, key ~p, val ~p",
                                    [BaseKey, I, Key, Val]),
                        error(badarg)
              end,
    Idx#sblob_idx{data=NewData}.

closest_loop(#sblob_idx{data=Data, base_key=BaseKey}=Idx, Key) ->
    I = Key - BaseKey,
    if
        I < 0 -> notfound;
        true ->
            Val = array:get(I, Data),
            case Val of
                undefined -> closest_loop(Idx, Key - 1);
                _ -> {Key, Val}
            end
    end.

% get Val from Key if found, otherwise return the closest value that is before
% Key, return notfound if Key doesn't exist and there's no smaller key.
% if Key is out of bounds it will throw badarg
closest(#sblob_idx{data=Data, base_key=BaseKey}=Idx, Key) ->
    I = Key - BaseKey,
    Val = try array:get(I, Data)
          catch error:badarg ->
                    lager:error("Error getting entry from index, base key ~p, i ~p, key ~p",
                                [BaseKey, I, Key]),
                    error(badarg)
          end,
    case Val of
        undefined -> closest_loop(Idx, Key - 1);
        _ -> {Key, Val}
    end.

% get the index that has the closest value to Val, if array is empty or there
% are no values set that are <= to Val return notfound.
% NOTE: see if using sparse_fold is faster than transversing the whole array
closest_value(#sblob_idx{data=Data, base_key=BaseKey}, Val) ->
    array:sparse_foldl(fun (I, CVal, Accum) ->
                               if CVal =< Val -> {I + BaseKey, CVal};
                                  true -> Accum
                               end
                       end, notfound, Data).

expand(#sblob_idx{data=Data}=Idx, Count) ->
    IndexSize = array:size(Data),
    NewIndexSize = IndexSize + Count,
    NewData = array:resize(NewIndexSize, Data),
    Idx#sblob_idx{data=NewData}.
