-module(gblob_triq).
-export([gblob_props/0]).

-include_lib("triq/include/triq.hrl").
-include("include/sblob.hrl").

char_data() -> oneof([int($a, $z), int($A, $Z), $ ]).

name_data() -> oneof([int($a, $z), int($A, $Z), int($0, $9)]).

data() -> ?LET(Length, int(0, 4096), begin vector(Length, char_data()) end).

timestamp() -> int(0, 1500000000000).

seqnum() -> pos_integer().
get_count() -> pos_integer().

gblob_config() -> [{max_items, int(1, 4096)}, {max_size_bytes, pos_integer()}].
gblob_name() -> ?LET(Length, int(1, 255),
                     begin
                         vector(Length, name_data())
                     end).

gblob_command() ->
    oneof([
           {put, data()},
           {put, timestamp(), data()},
           {get, seqnum()},
           {get, seqnum(), get_count()},
           stop
          ]).

execute_command(Pid, stop) ->
    R = gblob_server:stop(Pid),
    {R == stopped, nil};

execute_command(Pid, {put, Data}) ->
    BData = list_to_binary(Data),
    case gblob_server:put(Pid, BData) of
     #sblob_entry{data=BData} ->
            {true, Pid};
     Other ->
            {Other, Pid}
    end;

execute_command(Pid, {put, Ts, Data}) ->
    BData = list_to_binary(Data),
    case gblob_server:put(Pid, Ts, BData) of
     #sblob_entry{data=BData, timestamp=Ts} ->
            {true, Pid};
     Other ->
            {Other, Pid}
    end;

execute_command(Pid, {get, SN}) ->
    case gblob_server:get(Pid, SN) of
        #sblob_entry{seqnum=SN} -> {true, Pid};
        notfound -> {true, Pid};
        Other -> {Other, Pid}
    end;

execute_command(Pid, {get, SN, Count}) ->
    R = gblob_server:get(Pid, SN, Count),
    {is_list(R) orelse R == notfound orelse get2, Pid}.

execute_commands(_State, _Name, _Opts, []) ->
    ok;
execute_commands(nil, Name, Opts, Commands) ->
    Path = filename:join(["tmp", Name]),
    case gblob_server:start_link(Path, Opts) of
        {ok, Pid} -> execute_commands(Pid, Name, Opts, Commands);
        Other -> Other
    end;

execute_commands(Pid, Name, Opts, [Command|Commands]) ->
    case execute_command(Pid, Command) of
        {true, NewPid} ->
            execute_commands(NewPid, Name, Opts, Commands);
        Other -> Other
    end.


gblob_commands() -> ?LET(Length, pos_integer(),
                     begin
                         vector(Length, gblob_command())
                     end).

gblob_props() ->
    ?FORALL({Commands, Name, Opts}, {gblob_commands(), gblob_name(), gblob_config()},
            begin
                case execute_commands(nil, Name, Opts, Commands) of
                    ok -> true;
                    Other ->
                        io:format("failed ~p~n", [Other]),
                        false
                end
            end).
