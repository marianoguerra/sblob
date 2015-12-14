-module(gblob_server).
-behaviour(gen_server).

-export([start_link/2, start_link/3, stop/1, put/2, put/3, put/4, close/1,
         delete/1,
         get/2, get/3, status/1, truncate/2, truncate_percentage/2, size/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include("gblob.hrl").
-record(state, {gblob,
                last_action=0,
                last_check=0,
                last_eviction=0,
                active,
                max_interval_no_eviction_ms=60000,
                % NOTE: since status is called every 60 secs (by default)
                % for metrics if this is set to something above 60 secs it will
                % never call do_check
                check_interval_ms=30000}).

-define(CALL_TIMEOUT, 2000).

%% Public API

start_link(Path, GblobOpts) ->
    start_link(Path, GblobOpts, []).

start_link(Path, GblobOpts, ServerOpts) ->
    gen_server:start_link(?MODULE, [Path, GblobOpts, ServerOpts], []).

stop(Module) ->
    gen_server:call(Module, stop).

delete(Module) ->
    gen_server:call(Module, delete).

close(Module) ->
    gen_server:call(Module, close).

put(Pid, Data) ->
    gen_server:call(Pid, {put, Data}, ?CALL_TIMEOUT).

put(Pid, Timestamp, Data) ->
    gen_server:call(Pid, {put, Timestamp, Data}, ?CALL_TIMEOUT).

put(Pid, Timestamp, Data, LastSeqNum) ->
    gen_server:call(Pid, {put, Timestamp, Data, LastSeqNum}, ?CALL_TIMEOUT).

get(Pid, SeqNum) ->
    gen_server:call(Pid, {get, SeqNum}, ?CALL_TIMEOUT).

get(Pid, SeqNum, Count) ->
    gen_server:call(Pid, {get, SeqNum, Count}, ?CALL_TIMEOUT).

truncate(Pid, SizeBytes) ->
    gen_server:call(Pid, {truncate, SizeBytes}).

truncate_percentage(Pid, Percentage) when Percentage =< 1 ->
    gen_server:call(Pid, {truncate_percentage, Percentage}).

status(Pid) ->
    gen_server:call(Pid, status).

size(Pid) ->
    gen_server:call(Pid, size).

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

handle_call(delete, _From, State=#state{gblob=Gblob}) ->
    NewGblob = gblob:delete(Gblob),
    NewState = update_gblob(State, NewGblob, false),
    {reply, ok, NewState};

handle_call(close, _From, State=#state{gblob=Gblob}) ->
    NewGblob = gblob:close(Gblob),
    NewState = update_gblob(State, NewGblob, false),
    {reply, ok, NewState};

handle_call(status, _From, State=#state{active=Active,
                                        last_check=LastCheck,
                                        last_action=LastAction,
                                        last_eviction=LastEviction}) ->
    {reply, {Active, LastAction, LastEviction, LastCheck}, State,
     State#state.check_interval_ms};

handle_call({put, Data}, _From, State=#state{gblob=Gblob}) ->
    Timestamp = sblob_util:now(),
    server_put(Gblob, Timestamp, Data, nil, State);

handle_call({put, Timestamp, Data}, _From, State=#state{gblob=Gblob}) ->
    server_put(Gblob, Timestamp, Data, nil, State);

handle_call({put, Timestamp, Data, LastSeqNum}, _From, State=#state{gblob=Gblob}) ->
    server_put(Gblob, Timestamp, Data, LastSeqNum, State);

handle_call({get, SeqNum}, _From, State=#state{gblob=Gblob}) ->
    {NewGblob, Result} = gblob:get(Gblob, SeqNum),
    NewState = update_gblob(State, NewGblob),
    {reply, Result, NewState, State#state.check_interval_ms};

handle_call({get, SeqNum, Count}, _From, State=#state{gblob=Gblob}) ->
    {NewGblob, Result} = gblob:get(Gblob, SeqNum, Count),
    NewState = update_gblob(State, NewGblob),
    {reply, Result, NewState, State#state.check_interval_ms};

handle_call({truncate_percentage, Percentage}, _From, State=#state{gblob=Gblob}) ->
    {NewGblob, Result} = gblob:truncate_percentage(Gblob, Percentage),
    lager:info("truncate percentage ~s ~p", [NewGblob#gblob.path, Percentage]),
    NewState = update_gblob(State, NewGblob),
    {reply, Result, NewState, State#state.check_interval_ms};

handle_call(size, _From, State=#state{gblob=Gblob}) ->
    {NewGblob, Size} = gblob:size(Gblob),
    NewState = update_gblob(State, NewGblob),
    {reply, Size, NewState, State#state.check_interval_ms};

handle_call({truncate, SizeBytes}, _From, State=#state{gblob=Gblob}) ->
    {NewGblob, Result} = gblob:truncate(Gblob, SizeBytes),
    NewState = update_gblob(State, NewGblob),
    {reply, Result, NewState, State#state.check_interval_ms}.

handle_cast(Msg, State) ->
    io:format("Unexpected handle cast message: ~p~n",[Msg]),
    {noreply, State, State#state.check_interval_ms}.

handle_info(timeout, State=#state{active=false}) ->
    {noreply, State};

handle_info(timeout, State) ->
    {noreply, do_check(State), State#state.check_interval_ms};

handle_info(evict, State=#state{gblob=Gblob}) ->
    {noreply, evict(Gblob, State), State#state.check_interval_ms};

handle_info(Msg, State) ->
    io:format("Unexpected handle info message: ~p~n",[Msg]),
    {noreply, State, State#state.check_interval_ms}.


terminate(_Reason, _Gblob) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Private api

update_gblob(State, NewGblob) ->
    update_gblob(State, NewGblob, true).

update_gblob(State, NewGblob, Active) ->
    State#state{gblob=NewGblob, last_action=sblob_util:now_fast(), active=Active}.

check_eviction(State=#state{last_eviction=LastEviction,
                            max_interval_no_eviction_ms=MaxIntervalNoEvictionMs}) ->

    Now = sblob_util:now_fast(),
    LastEvictionTime = Now - MaxIntervalNoEvictionMs,
    % if last_eviction is 0 ignore and set to something small so next time it
    % runs
    ShouldEvict = (LastEviction /= 0) andalso (LastEviction < LastEvictionTime),
    NewEvictionTime = if
                          LastEviction == 0 -> Now;
                          true -> LastEviction
                      end,
    NewState = State#state{last_eviction=NewEvictionTime, last_check=Now},
    {ShouldEvict, NewState}.

evict(Gblob, State) ->
    Now = sblob_util:now_fast(),
    NewGblob = do_eviction(Gblob),
    State#state{gblob=NewGblob, last_eviction=Now}.

put_blob(Gblob, Timestamp, Data, nil) ->
    gblob:put(Gblob, Timestamp, Data);
put_blob(Gblob, Timestamp, Data, LastSeqNum) ->
    case gblob:put(Gblob, Timestamp, Data, LastSeqNum) of
        {error, Reason, NewGblob} ->
            {NewGblob, {error, Reason}};
        Other -> Other
    end.

server_put(Gblob, Timestamp, Data, LastSeqNum, State) ->
    {Gblob1, Reply} = put_blob(Gblob, Timestamp, Data, LastSeqNum),
    State1 = update_gblob(State, Gblob1),
    {ShouldEvict, NewState} = check_eviction(State1),
    if ShouldEvict -> timer:send_after(1, evict);
       true -> ok
    end,
    {reply, Reply, NewState, NewState#state.check_interval_ms}.

do_eviction(Gblob=#gblob{path=Path}) ->
    {MaybeEvictedGblob, EvictionResult} = gblob:check_eviction(Gblob),
    gblob_util:log_eviction_results(Path, EvictionResult),
    MaybeEvictedGblob.

do_check(State=#state{gblob=Gblob, last_action=LastAction, check_interval_ms=CheckIntervalMs}) ->
    Path = Gblob#gblob.path,

    Now = sblob_util:now_fast(),
    LastCheckTime = Now - CheckIntervalMs,
    ShouldClose = LastAction < LastCheckTime,

    {NewActive, NewGblob} = if ShouldClose ->
                                   lager:debug("closing inactive gblob ~s", [Path]),
                                   {false, gblob:close(Gblob)};
                               true ->
                                   {true, Gblob}
                            end,

    MaybeEvictedGblob = do_eviction(NewGblob),

    % set last_eviction to 0 so check_eviction doesn't run next time
    State#state{active=NewActive, gblob=MaybeEvictedGblob, last_check=Now,
                last_eviction=0}.
