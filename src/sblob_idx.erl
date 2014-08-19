-module(sblob_idx).
-export([new/1, put/3, closest/2]).

-record(sblob_idx, {base_key=0, data}).
-define(SBLOB_IDX_DEFAULT_INDEX_SIZE, 256).

new(BaseKey) ->
    Data = array:new(?SBLOB_IDX_DEFAULT_INDEX_SIZE),
    #sblob_idx{base_key=BaseKey, data=Data}.

put(#sblob_idx{data=Data, base_key=BaseKey}=Idx, Key, Val) ->
    I = Key - BaseKey,
    NewData = array:set(I, Val, Data),
    Idx#sblob_idx{data=NewData}.

closest_loop(#sblob_idx{data=Data, base_key=BaseKey}=Idx, Key) ->
    I = Key - BaseKey,
    if
        I < 0 -> notfound;
        true ->
            Val = array:get(I, Data),
            case Val of
                undefined -> closest_loop(Idx, Key - 1);
                _ -> Val
            end
    end.

% get Val from Key if found, otherwise return the closest value that is before
% Key, return notfound if Key doesn't exist and there's no smaller key.
% if Key is out of bounds it will throw badarg
closest(#sblob_idx{data=Data, base_key=BaseKey}=Idx, Key) ->
    I = Key - BaseKey,
    Val = array:get(I, Data),
    case Val of
        undefined -> closest_loop(Idx, Key -1);
        _ -> Val
    end.

