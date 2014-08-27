-module(gblob_server).
-behaviour(gen_server).

-export([start/2, stop/1, state/1, put/2, get/2, get/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Public API

start(Path, Opts) ->
    gen_server:start(?MODULE, [Path, Opts], []).

stop(Module) ->
    gen_server:call(Module, stop).

put(Pid, Data) ->
    gen_server:call(Pid, {put, Data}).

get(Pid, SeqNum) ->
    gen_server:call(Pid, {get, SeqNum}).

get(Pid, SeqNum, Count) ->
    gen_server:call(Pid, {get, SeqNum, Count}).

state(Module) ->
    gen_server:call(Module, state).

%% Server implementation, a.k.a.: callbacks

init([Path, Opts]) ->
    Gblob = gblob:open(Path, Opts),
    {ok, Gblob}.

handle_call(stop, _From, Gblob) ->
    NewGblob = gblob:close(Gblob),
    {stop, normal, stopped, NewGblob};

handle_call(state, _From, Gblob) ->
    {reply, Gblob, Gblob};

handle_call({put, Data}, _From, Gblob) ->
    {NewGblob, Entity} = gblob:put(Gblob, Data),
    {reply, Entity, NewGblob};

handle_call({get, SeqNum}, _From, Gblob) ->
    {NewGblob, Result} = gblob:get(Gblob, SeqNum),
    {reply, Result, NewGblob};

handle_call({get, SeqNum, Count}, _From, Gblob) ->
    {NewGblob, Result} = gblob:get(Gblob, SeqNum, Count),
    {reply, Result, NewGblob}.

handle_cast(Msg, Gblob) ->
    io:format("Unexpected handle cast message: ~p~n",[Msg]),
    {noreply, Gblob}.


handle_info(Msg, Gblob) ->
    io:format("Unexpected handle info message: ~p~n",[Msg]),
    {noreply, Gblob}.


terminate(_Reason, _Gblob) ->
    ok.

code_change(_OldVsn, Gblob, _Extra) ->
    {ok, Gblob}.

