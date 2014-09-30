-module(gblob_bucket).
-behaviour(gen_server).

-export([start/3, stop/1, state/1, put/3, put/4, get/3, get/4,
         truncate_percentage/2, size/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {gblobs, gblob_opts, bucket_opts, path}).
-record(bucket_cfg, {max_items}).

%% Public API

start(Path, GblobOpts, BucketOpts) ->
    gen_server:start(?MODULE, [Path, GblobOpts, BucketOpts], []).

stop(Module) ->
    gen_server:call(Module, stop).

put(Pid, Id, Data) when is_binary(Id) ->
    gen_server:call(Pid, {put, Id, Data}).

put(Pid, Id, Timestamp, Data) when is_binary(Id) ->
    gen_server:call(Pid, {put, Id, Timestamp, Data}).

get(Pid, Id, SeqNum) when is_binary(Id) ->
    gen_server:call(Pid, {get, Id, SeqNum}).

get(Pid, Id, SeqNum, Count) when is_binary(Id) ->
    gen_server:call(Pid, {get, Id, SeqNum, Count}).

truncate_percentage(Pid, Percentage) when Percentage =< 1 ->
    gen_server:call(Pid, {truncate_percentage, Percentage}).

size(Pid) ->
    gen_server:call(Pid, size).

state(Module) ->
    gen_server:call(Module, state).

%% Server implementation, a.k.a.: callbacks

init([Path, GblobOpts, BucketOpts]) ->
    Gblobs = sblob_preg:new(),
    BucketConfig = parse_opts(BucketOpts),
    State = #state{gblobs=Gblobs, gblob_opts=GblobOpts,
                   bucket_opts=BucketConfig, path=Path},
    {ok, State}.


handle_call(stop, _From, State) ->
    NewState = foreach_active_gblob(State,
                             fun (_Id, Gblob) ->
                                     gblob_server:stop(Gblob)
                             end),
    {stop, normal, stopped, NewState};

handle_call(state, _From, State) ->
    {reply, State, State};

handle_call({put, Id, Data}, _From, State) ->
    with_gblob(State, Id, fun(Gblob) -> gblob_server:put(Gblob, Data) end);

handle_call({put, Id, Timestamp, Data}, _From, State) ->
    with_gblob(State, Id, fun(Gblob) -> gblob_server:put(Gblob, Timestamp, Data) end);

handle_call({get, Id, SeqNum}, _From, State) ->
    with_gblob(State, Id, fun(Gblob) -> gblob_server:get(Gblob, SeqNum) end);

handle_call({get, Id, SeqNum, Count}, _From, State) ->
    with_gblob(State, Id, fun(Gblob) -> gblob_server:get(Gblob, SeqNum, Count) end);

handle_call({truncate_percentage, Percentage}, _From, State) ->
    TruncateGblobs = fun ({_Id, Gblob}) ->
                             gblob_server:truncate_percentage(Gblob, Percentage)
                     end,
    {NewState, Result} = map_gblobs(State, TruncateGblobs),
    {reply, lists:reverse(Result), NewState};

handle_call(size, _From, State) ->
    GetSizes = fun ({Id, Gblob}, {CurTotalSize, CurSizes}) ->
                       Size = gblob_server:size(Gblob),
                       NewTotalSize = CurTotalSize + Size,
                       NewCurSizes = [{Id, Size}|CurSizes],
                       {NewTotalSize, NewCurSizes}
               end,

    {NewState, Result} = foldl_gblobs(State, GetSizes, {0, []}),
    {reply, Result, NewState}.

handle_cast(Msg, State) ->
    io:format("Unexpected handle cast message: ~p~n",[Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    io:format("Unexpected handle info message: ~p~n",[Msg]),
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal

parse_opts(Opts) ->
    parse_opts(Opts, #bucket_cfg{}).


parse_opts([], Config) ->
    Config;

parse_opts([{max_items, Val}|T], Config) ->
    parse_opts(T, Config#bucket_cfg{max_items=Val}).

with_gblob(State, Id, Fun) ->
    {NewState, Gblob} = get_gblob(State, Id),
    Result = Fun(Gblob),
    {reply, Result, NewState}.

create_gblob(Gblobs, Path, Opts, Id) ->
    {ok, Gblob} = gblob_server:start(Path, Opts),
    NewGblobs = sblob_preg:put(Gblobs, Id, Gblob),
    {NewGblobs, Gblob}.

to_list(Data) when is_list(Data) -> Data;
to_list(Data) when is_binary(Data) -> binary_to_list(Data).

get_gblob(#state{gblobs=Gblobs, gblob_opts=Opts, path=Path}=State, Id) ->
    {NewGblobs, Gblob} = case sblob_preg:get(Gblobs, Id) of
        none ->
            IdStr = to_list(Id),
            GblobPath = filename:join([Path, IdStr]),
            create_gblob(Gblobs, GblobPath, Opts, Id);
        {value, FoundGblob} -> {Gblobs, FoundGblob}
    end,
    {State#state{gblobs=NewGblobs}, Gblob}.

foreach_active_gblob(#state{gblobs=Gblobs}=State, Fun) ->
    sblob_preg:foreach(Gblobs, Fun),
    State.

get_gblob_names(Path) ->
    {ok, SubFiles} = file:list_dir(Path),
    lists:map(fun list_to_binary/1, SubFiles).

map_gblobs(State, Fun) ->
    foldl_gblobs(State, fun (Item, Results) ->
                             Result = Fun(Item),
                             [Result|Results]
                        end, []).

foldl_gblobs(State=#state{path=Path}, Fun, Acc0) ->
    Ids = get_gblob_names(Path),
    WrapperFun = fun (Id, {BaseState, Accum}) ->
                         {NewState, Gblob} = get_gblob(BaseState, Id),
                         NewAccum = Fun({Id, Gblob}, Accum),
                         {NewState, NewAccum}
                 end,
    {FinalState, Accum} = lists:foldl(WrapperFun, {State, Acc0}, Ids),
    {FinalState, Accum}.
