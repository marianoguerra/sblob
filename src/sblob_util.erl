-module(sblob_util).
-export([parse_config/1, now/0,
         get_handle/1, seek/2, seek_to_seqnum/2,
         clear_data/1, read/2, remove_folder/1,
         to_binary/1, to_binary/3, from_binary/1, header_from_binary/1,
         blob_size/1, offset_for_seqnum/2, fill_size/1]).

-include("sblob.hrl").

-define(SBLOB_CURRENT_CHUNK_NAME, "current").

now() ->
    {Mega, Sec, Micro} = erlang:now(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.

parse_config(Opts) ->
    parse_config(Opts, #sblob_cfg{}).

parse_config([{read_ahead, Val}|T], Config) ->
    parse_config(T, Config#sblob_cfg{read_ahead=Val});

parse_config([{max_items, Val}|T], Config) ->
    parse_config(T, Config#sblob_cfg{max_items=Val});

parse_config([{max_age_ms, Val}|T], Config) ->
    parse_config(T, Config#sblob_cfg{max_age_ms=Val});

parse_config([{max_size_bytes, Val}|T], Config) ->
    parse_config(T, Config#sblob_cfg{max_size_bytes=Val});

parse_config([{base_seqnum, Val}|T], Config) ->
    parse_config(T, Config#sblob_cfg{base_seqnum=Val});

parse_config([], Config) ->
    Config.

% return the file handle to the current chunk, if not open already open it
% and store it in the returned Sblob record
get_handle(#sblob{fullpath=FullPath,
                  config=#sblob_cfg{read_ahead=ReadAhead}}=Sblob) ->
    Path = filename:join([FullPath, ?SBLOB_CURRENT_CHUNK_NAME]),
    {ok, Handle} = file:open(Path, [append, read, raw, binary,
                                    {read_ahead, ReadAhead}]),
    {Handle, Sblob#sblob{handle=Handle, position=0}}.

% like get_handle but will seek the handle to the required place,
% Location is the same as file:position
seek(#sblob{handle=nil}=Sblob, Location) ->
    {_, Sblob1} = get_handle(Sblob),
    seek(Sblob1, Location);
seek(#sblob{handle=Handle}=Sblob, Location) ->
    {ok, NewPos} = file:position(Handle, Location),
    Sblob#sblob{position=NewPos}.

seek_to_seqnum(Sblob, SeqNum) ->
    {Handle, NewSblob} = get_handle(Sblob),
    {OffsetKey, Offset} =  case offset_for_seqnum(Sblob, SeqNum) of
        notfound -> {nil, 0};
        Result -> Result
    end,
    {ok, NewPos} = file:position(Handle, {bof, Offset}),
    {OffsetKey, NewSblob#sblob{position=NewPos}}.

fill_size(Sblob) ->
    NewSblob = seek(Sblob, eof),
    Pos = NewSblob#sblob.position,
    NewSblob#sblob{size=Pos}.

read(#sblob{handle=nil}=Sblob, Len) ->
    {_, NewSblob} = get_handle(Sblob),
    read(NewSblob, Len);

read(#sblob{position=Pos, handle=Handle}=Sblob, Len) ->
    {Sblob#sblob{position=Pos + Len}, file:read(Handle, Len)}.

to_binary(#sblob_entry{timestamp=Timestamp, seqnum=SeqNum, data=Data}) ->
    to_binary(Timestamp, SeqNum, Data).

to_binary(Timestamp, SeqNum, Data) ->
    Len = size(Data),
    <<Timestamp:64/integer, SeqNum:64/integer, Len:32/integer, Data/binary,
    Len:32/integer>>.

from_binary(<<Timestamp:64/integer, SeqNum:64/integer, Len:32/integer, Tail/binary>>) ->
    Data = binary:part(Tail, 0, Len),
    #sblob_entry{timestamp=Timestamp, seqnum=SeqNum, len=Len, data=Data}.

header_from_binary(<<Timestamp:64/integer, SeqNum:64/integer, Len:32/integer, _Tail/binary>>) ->
    #sblob_entry{timestamp=Timestamp, seqnum=SeqNum, len=Len, data=nil}.

clear_data(Entry) ->
    Entry#sblob_entry{data=nil}.

% returns the size of the complete blob entry in disk (header, data and footer)
blob_size(EntryDataLen) ->
    HeaderSize = ?SBLOB_HEADER_SIZE_BYTES,
    LenSize = ?SBLOB_HEADER_LEN_SIZE_BYTES,
    EntryDataLen + LenSize + HeaderSize.

remove_folder(Path) ->
    % TODO: delete instead of move
    NowStr = integer_to_list(sblob_util:now()),
    file:rename(Path, Path ++ "." ++ NowStr ++ ".removed").

offset_for_seqnum(#sblob{index=Idx}, SeqNum) -> 
    sblob_idx:closest(Idx, SeqNum).
