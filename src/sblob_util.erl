-module(sblob_util).
-export([parse_config/1, now/0, get_handle/1,
         to_binary/1, to_binary/3, from_binary/1]).

-include("sblob.hrl").

-define(SBLOB_CURRENT_CHUNK_NAME, "current").

now() ->
    {Mega, Sec, Micro} = erlang:now(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.

parse_config(Opts) ->
    parse_config(Opts, #sblob_cfg{}).

parse_config([], Config) ->
    Config.

get_handle(#sblob{fullpath=FullPath,
                  config=#sblob_cfg{read_ahead=ReadAhead}}=Sblob) ->
    Path = filename:join([FullPath, ?SBLOB_CURRENT_CHUNK_NAME]),
    {ok, Handle} = file:open(Path, [append, raw, binary, {read_ahead, ReadAhead}]),
    {Handle, Sblob#sblob{handle=Handle}}.

to_binary(#sblob_entry{timestamp=Timestamp, seqnum=SeqNum, data=Data}) ->
    to_binary(Timestamp, SeqNum, Data).

to_binary(Timestamp, SeqNum, Data) ->
    <<Timestamp:64/integer, SeqNum:64/integer, Data/binary>>.

from_binary(<<Timestamp:64/integer, SeqNum:64/integer, Data/binary>>) ->
    #sblob_entry{timestamp=Timestamp, seqnum=SeqNum, data=Data}.
