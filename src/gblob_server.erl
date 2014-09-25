-module(gblob_server).
-behaviour(gen_server).

-export([start/2, start/3, stop/1, put/2, put/3, get/2, get/3, status/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("gblob.hrl").
-record(state, {gblob,
                last_action=0,
                last_check=0,
                last_eviction_check=0,
                active,
                max_interval_no_eviction_ms = 60000,
                check_interval_ms=30000}).

%% Public API

start(Path, GblobOpts) ->
    start(Path, GblobOpts, []).

start(Path, GblobOpts, ServerOpts) ->
    gen_server:start(?MODULE, [Path, GblobOpts, ServerOpts], []).

stop(Module) ->
    gen_server:call(Module, stop).

put(Pid, Data) ->
    gen_server:call(Pid, {put, Data}).

put(Pid, Timestamp, Data) ->
    gen_server:call(Pid, {put, Timestamp, Data}).

get(Pid, SeqNum) ->
    gen_server:call(Pid, {get, SeqNum}).

get(Pid, SeqNum, Count) ->
    gen_server:call(Pid, {get, SeqNum, Count}).

status(Pid) ->
    gen_server:call(Pid, status).

%% Server implementation, a.k.a.: callbacks

init([Path, Opts, ServerOpts]) ->
    CheckIntervalMs = proplists:get_value(check_interval_ms, ServerOpts, 30000),
    MaxIntervalNoEvictionMs = proplists:get_value(max_interval_no_eviction_ms, ServerOpts, 60000),
    Gblob = gblob:open(Path, Opts),
    BaseState = #state{check_interval_ms=CheckIntervalMs,
                       max_interval_no_eviction_ms=MaxIntervalNoEvictionMs},
    State = update_gblob(BaseState, Gblob),
    {ok, State, State#state.check_interval_ms}.

handle_call(stop, _From, State=#state{gblob=Gblob}) ->
    NewGblob = gblob:close(Gblob),
    NewState = update_gblob(State, NewGblob, false),
    {stop, normal, stopped, NewState};

handle_call(status, _From, State=#state{active=Active,
                                        last_action=LastAction,
                                        last_eviction_check=LastEviction}) ->
    {reply, {Active, LastAction, LastEviction}, State};

handle_call({put, Data}, _From, State=#state{gblob=Gblob}) ->
    Timestamp = sblob_util:now(),
    server_put(Gblob, Timestamp, Data, State);

handle_call({put, Timestamp, Data}, _From, State=#state{gblob=Gblob}) ->
    server_put(Gblob, Timestamp, Data, State);

handle_call({get, SeqNum}, _From, State=#state{gblob=Gblob}) ->
    {NewGblob, Result} = gblob:get(Gblob, SeqNum),
    NewState = update_gblob(State, NewGblob),
    {reply, Result, NewState, State#state.check_interval_ms};

handle_call({get, SeqNum, Count}, _From, State=#state{gblob=Gblob}) ->
    {NewGblob, Result} = gblob:get(Gblob, SeqNum, Count),
    NewState = update_gblob(State, NewGblob),
    {reply, Result, NewState, State#state.check_interval_ms}.

handle_cast(Msg, State) ->
    io:format("Unexpected handle cast message: ~p~n",[Msg]),
    {noreply, State}.

handle_info(timeout, State=#state{active=false}) ->
   {noreply, State};

handle_info(timeout, State) ->
    {noreply, do_check(State)};

handle_info(Msg, State) ->
    io:format("Unexpected handle info message: ~p~n",[Msg]),
    {noreply, State}.


terminate(_Reason, _Gblob) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Private api

update_gblob(State, NewGblob) ->
    update_gblob(State, NewGblob, true).

update_gblob(State, NewGblob, Active) ->
    State#state{gblob=NewGblob, last_action=sblob_util:now_fast(), active=Active}.

check_eviction(Gblob=#gblob{path=Path}, State=#state{last_eviction_check=LastCheck,
                                   max_interval_no_eviction_ms=MaxIntervalNoEvictionMs}) ->
    Now = sblob_util:now_fast(),
    LastCheckTime = Now - MaxIntervalNoEvictionMs,
    ShouldEvict = LastCheck /= 0 andalso LastCheck < LastCheckTime,

    NewState = State#state{last_eviction_check=Now},

    if
        ShouldEvict ->
            lager:info("max interval with no eviction, calling ~s", [Path]),
            NewGblob = do_eviction(Gblob),
            NewState#state{gblob=NewGblob};
        true -> NewState
    end.


server_put(Gblob, Timestamp, Data, State) ->
    {Gblob1, Entity} = gblob:put(Gblob, Timestamp, Data),
    State1 = update_gblob(State, Gblob1),
    NewState = check_eviction(Gblob1, State1),
    {reply, Entity, NewState, NewState#state.check_interval_ms}.

do_eviction(Gblob=#gblob{path=Path}) ->
    {MaybeEvictedGblob, EvictionResult} = gblob:check_eviction(Gblob),
    gblob_util:log_eviction_results(Path, EvictionResult),
    MaybeEvictedGblob.

do_check(State=#state{gblob=Gblob, last_action=LastAction, check_interval_ms=CheckIntervalMs}) ->
    Path = Gblob#gblob.path,

    Now = sblob_util:now_fast(),
    LastCheckTime = Now - CheckIntervalMs,
    ShouldClose = LastAction < LastCheckTime,

    {NewActive, NewGblob} = if
                                ShouldClose ->
                                    lager:info("closing inactive gblob ~s", [Path]),
                                    {false, gblob:close(Gblob)};
                                true ->
                                    {true, Gblob}
                            end,

    MaybeEvictedGblob = do_eviction(NewGblob),

    State#state{active=NewActive, gblob=MaybeEvictedGblob, last_check=Now,
               last_eviction_check=Now}.
