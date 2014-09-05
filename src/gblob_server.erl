-module(gblob_server).
-behaviour(gen_server).

-export([start/2, start/3, stop/1, put/2, put/3, get/2, get/3, status/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("gblob.hrl").
-record(state, {gblob, last_action, active, check_interval_ms=30000}).

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
    Gblob = gblob:open(Path, Opts),
    BaseState = #state{active=true, check_interval_ms=CheckIntervalMs},
    State = update_gblob(BaseState, Gblob),
    {ok, State, State#state.check_interval_ms}.

handle_call(stop, _From, State=#state{gblob=Gblob}) ->
    NewGblob = gblob:close(Gblob),
    NewState = update_gblob(State, NewGblob),
    {stop, normal, stopped, NewState};

handle_call(status, _From, State=#state{active=Active, last_action=LastAction}) ->
    {reply, {Active, LastAction}, State};

handle_call({put, Data}, _From, State=#state{gblob=Gblob}) ->
    {NewGblob, Entity} = gblob:put(Gblob, Data),
    NewState = update_gblob(State, NewGblob),
    {reply, Entity, NewState, State#state.check_interval_ms};

handle_call({put, Timestamp, Data}, _From, State=#state{gblob=Gblob}) ->
    {NewGblob, Entity} = gblob:put(Gblob, Timestamp, Data),
    NewState = update_gblob(State, NewGblob),
    {reply, Entity, NewState, State#state.check_interval_ms};

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

handle_info(timeout, State=#state{gblob=Gblob, last_action=LastAction, check_interval_ms=CheckIntervalMs}) ->
   lager:info("closing inactive gblob ~s", [Gblob#gblob.path]),

   Now = sblob_util:now_fast(),
   LastCheckTime = Now - CheckIntervalMs,
   ShouldClose = LastAction < LastCheckTime,

   {NewActive, NewGblob} = if ShouldClose -> {false, gblob:close(Gblob)};
                 true -> {true, Gblob}
              end,
   {noreply, State#state{active=NewActive, gblob=NewGblob}};

handle_info(Msg, State) ->
    io:format("Unexpected handle info message: ~p~n",[Msg]),
    {noreply, State}.


terminate(_Reason, _Gblob) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Private api

update_gblob(State, NewGblob) ->
    State#state{gblob=NewGblob, last_action=sblob_util:now_fast()}.
