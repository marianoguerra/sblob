-module(sblob_util).
-export([parse_config/1, now/0, now_fast/0,
         get_handle/1, seek/2, seek_to_seqnum/2,
         clear_data/1, read/2, remove/1, mark_removed/1,
         deep_size/1,
         get_blob_info/3,
         handle_get_one/1, seqread/5, fold/5,
         get_next/1, get_first/1, get_last/1, read_until/4,
         to_binary/1, to_binary/3, from_binary/1, header_from_binary/1,
         blob_size/1, offset_for_seqnum/2, fill_bounds/1]).

-include("sblob.hrl").

-include_lib("kernel/include/file.hrl").

% it makes no sense to use erlang:now() since we are dividing by 1000
now() -> now_fast().

% If you do not need the return value to be unique and monotonically
% increasing, use os:timestamp/0 instead to avoid some overhead.
now_fast() ->
    {Mega, Sec, Micro} = os:timestamp(),
    ((Mega * 1000000 + Sec) * 1000000 + Micro) div 1000.

parse_config(Opts) ->
    parse_config(Opts, #sblob_cfg{}).

parse_config([{read_ahead, Val}|T], Config) ->
    parse_config(T, Config#sblob_cfg{read_ahead=Val});

parse_config([{max_items, Val}|T], Config) ->
    parse_config(T, Config#sblob_cfg{max_items=Val});

parse_config([{base_seqnum, Val}|T], Config) ->
    parse_config(T, Config#sblob_cfg{base_seqnum=Val});

parse_config([], Config) ->
    Config.

open_file(Path, _ReadAhead) ->
    open_file(Path, _ReadAhead, file_handle_cache).

open_file(Path, _ReadAhead, file_handle_cache) ->
    % XXX ignore ReadAhead for now since not on file_handle_cache
    % TODO: see last opetions to open
    {ok, Handle} = file_handle_cache:open(Path, [raw, binary, read, append], []),
    Handle;

open_file(Path, _ReadAhead, file) ->
    % XXX ignore ReadAhead for now since not on file_handle_cache
    % TODO: see last opetions to open
    {ok, Handle} = file:open(Path, [raw, binary, read, append]),
    Handle.

% return the file handle to the current chunk, if not open already open it
% and store it in the returned Sblob record
get_handle(#sblob{fullpath=FullPath, handle=nil,
                  config=#sblob_cfg{read_ahead=ReadAhead}}=Sblob) ->
    lager:debug("get handle ~s", [FullPath]),
    Handle = open_file(FullPath, ReadAhead),
    {ok, Size} = file_handle_cache:position(Handle, eof),
    {Handle, Sblob#sblob{handle=Handle, position=Size, size=Size}};

get_handle(#sblob{handle=Handle}=Sblob) ->
    {Handle, Sblob}.

% like get_handle but will seek the handle to the required place,
% Location is the same as file:position
seek(#sblob{handle=nil}=Sblob, Location) ->
    {_, Sblob1} = get_handle(Sblob),
    seek(Sblob1, Location);
seek(#sblob{handle=Handle, name=Name}=Sblob, Location) ->
    lager:debug("seek ~s ~p", [Name, Location]),
    case file_handle_cache:position(Handle, Location) of
        {ok, NewPos} -> Sblob#sblob{position=NewPos};
        Error ->
            lager:error("error seeking sblob ~p to ~p: ~p",
                        [Name, Location, Error]),
            sblob:close(Sblob)
    end.

seek_to_seqnum(Sblob, SeqNum) ->
    {Handle, NewSblob} = get_handle(Sblob),
    {OffsetKey, Offset} =  case offset_for_seqnum(Sblob, SeqNum) of
        notfound -> {nil, 0};
        Result -> Result
    end,
    {ok, NewPos} = file_handle_cache:position(Handle, {bof, Offset}),
    {OffsetKey, NewSblob#sblob{position=NewPos}}.

fill_bounds(#sblob{name=Name}=Sblob) ->
    {Sblob1, First} = get_first(Sblob),
    Cfg = Sblob1#sblob.config,
    CfgBaseSeqNum = Cfg#sblob_cfg.base_seqnum,
    MaxItems = Cfg#sblob_cfg.max_items,
    case First of
        notfound -> 
            lager:debug("fill bounds, empty ~s", [Name]),
            Index = sblob_idx:new(CfgBaseSeqNum + 1, MaxItems),
            Sblob1#sblob{base_seqnum=CfgBaseSeqNum, seqnum=CfgBaseSeqNum,
                         size=0, index=Index};
        #sblob_entry{seqnum=FirstSeqNum, offset=FirstOffset} ->
            {Sblob2, #sblob_entry{seqnum=LastSeqNum, offset=LastOffset}} = get_last(Sblob1),
            BaseSeqNum = FirstSeqNum  - 1,
            lager:debug("fill bounds ~s ~p - ~p", [Name, BaseSeqNum, LastSeqNum]),
            Index = sblob_idx:new(BaseSeqNum + 1, MaxItems),
            Index1 = sblob_idx:put(Index, FirstSeqNum, FirstOffset),
            Index2 = sblob_idx:put(Index1, LastSeqNum, LastOffset),
            Sblob2#sblob{base_seqnum=BaseSeqNum, seqnum=LastSeqNum, index=Index2}
    end.

read(#sblob{handle=nil}=Sblob, Len) ->
    {_, NewSblob} = get_handle(Sblob),
    read(NewSblob, Len);

read(#sblob{position=Pos, handle=Handle}=Sblob, Len) ->
    {Sblob#sblob{position=Pos + Len}, file_handle_cache:read(Handle, Len)}.

to_binary(#sblob_entry{timestamp=Timestamp, seqnum=SeqNum, data=Data}) ->
    to_binary(Timestamp, SeqNum, Data).

to_binary(Timestamp, SeqNum, Data) ->
    Len = size(Data),
    <<Timestamp:64/integer, SeqNum:64/integer, Len:32/integer, Data/binary,
    Len:32/integer>>.

from_binary(<<Timestamp:64/integer, SeqNum:64/integer, Len:32/integer, Tail/binary>>) ->
    Data = binary:part(Tail, 0, Len),
    #sblob_entry{timestamp=Timestamp, seqnum=SeqNum, len=Len, data=Data,
                 size=?SBLOB_HEADER_SIZE_BYTES + size(Tail)}.

header_from_binary(<<Timestamp:64/integer, SeqNum:64/integer, Len:32/integer, _Tail/binary>>) ->
    #sblob_entry{timestamp=Timestamp, seqnum=SeqNum, len=Len, data=nil}.

clear_data(Entry) ->
    Entry#sblob_entry{data=nil}.

% returns the size of the complete blob entry in disk (header, data and footer)
blob_size(EntryDataLen) ->
    HeaderSize = ?SBLOB_HEADER_SIZE_BYTES,
    LenSize = ?SBLOB_HEADER_LEN_SIZE_BYTES,
    EntryDataLen + LenSize + HeaderSize.

mark_removed(Path) ->
    NowStr = integer_to_list(sblob_util:now()),
    case file:rename(Path, Path ++ "." ++ NowStr ++ ".removed") of
        ok -> ok;
        {error, enoent} -> ok
    end.

offset_for_seqnum(#sblob{index=Idx}, SeqNum) -> 
    Result = sblob_idx:closest(Idx, SeqNum),
    lager:debug("offset for seqnum ~p: ~p", [SeqNum, Result]),
    Result.

raw_get_next(Handle) ->
     case file:read(Handle, ?SBLOB_HEADER_SIZE_BYTES) of
         {ok, Header} ->

             HeaderEntry = header_from_binary(Header),
             Len = HeaderEntry#sblob_entry.len,
             {ok, Tail} = file:read(Handle, Len + ?SBLOB_HEADER_LEN_SIZE_BYTES),
             Data = binary:part(Tail, 0, Len),
             Entry = HeaderEntry#sblob_entry{data=Data,
                                             size=?SBLOB_HEADER_SIZE_BYTES + size(Tail)},
             Entry;
         eof -> eof
     end.

get_next(#sblob{position=Pos, size=Pos}=Sblob) -> {Sblob, notfound};

get_next(Sblob) ->
    {Sblob1, {ok, Header}} = read(Sblob, ?SBLOB_HEADER_SIZE_BYTES),
    HeaderEntry = header_from_binary(Header),
    Len = HeaderEntry#sblob_entry.len,
    {Sblob2, {ok, Tail}} = read(Sblob1, Len + ?SBLOB_HEADER_LEN_SIZE_BYTES),
    Data = binary:part(Tail, 0, Len),
    EntryOffset = Sblob#sblob.position,
    Entry = HeaderEntry#sblob_entry{data=Data,
                                    offset=EntryOffset,
                                    size=?SBLOB_HEADER_SIZE_BYTES + size(Tail)},

    BlobSeqNum = Entry#sblob_entry.seqnum,
    Index = Sblob2#sblob.index,
    NewIndex = if
        Index == nil -> Index;
        true ->
            sblob_idx:put(Index, BlobSeqNum, EntryOffset)
    end,
    Sblob3 = Sblob2#sblob{index=NewIndex},

    {Sblob3, Entry}.

get_first(#sblob{fullpath=FullPath}=Sblob) ->
    lager:debug("get_first ~s", [FullPath]),
    NewSblob = seek(Sblob, bof),
    get_next(NewSblob).

get_last(#sblob{fullpath=FullPath}=Sblob) ->
    lager:debug("get_last ~s", [FullPath]),
    LenSize = ?SBLOB_HEADER_LEN_SIZE_BYTES,

    Sblob1 = seek(Sblob, {eof, -LenSize}),
    {Sblob2, LenData} = read(Sblob1, LenSize),
    % since we read the last 4 bytes for the entry len we are at the end,
    % that means that now position == size, we use it to set the blob size
    SblobSize = Sblob2#sblob.position,
    {ok, <<EntryDataLen:?SBLOB_HEADER_LEN_SIZE_BITS/integer>>} = LenData,

    Offset = blob_size(EntryDataLen),

    Sblob3 = seek(Sblob2#sblob{size=SblobSize}, {cur, -Offset}),
    get_next(Sblob3).


read_until(#sblob{name=Name}=Sblob, CurSeqNum, TargetSeqNum, Accumulate) ->
    lager:debug("read_until ~s ~p ~p", [Name, CurSeqNum, TargetSeqNum]),
    read_until(Sblob, CurSeqNum, TargetSeqNum, Accumulate, []).


read_until(Sblob, CurSeqNum, TargetSeqNum, _Accumulate, Accum)
  when CurSeqNum >= TargetSeqNum ->
    {Sblob, CurSeqNum, lists:reverse(Accum)};

read_until(#sblob{size=Size, position=Size}=Sblob, CurSeqNum, _TargetSeqNum, _Accumulate, Accum) ->
    {Sblob, CurSeqNum, lists:reverse(Accum)};

read_until(Sblob, CurSeqNum, TargetSeqNum, Accumulate, Accum) ->
    case get_next(Sblob) of
        {TSblob1, notfound} -> {TSblob1, CurSeqNum, lists:reverse(Accum)};
        {Sblob1, Blob} ->
            NewAccum = if Accumulate -> [Blob|Accum];
                          true -> Accum
                       end,

            read_until(Sblob1, Blob#sblob_entry.seqnum + 1, TargetSeqNum, Accumulate, NewAccum)
    end.

handle_get_one(Result) ->
    case Result of
        {First, []} -> {First, notfound};
        {First, [Entry]} -> {First, Entry}
    end.

% stop if more than count
seqread_fold_fun(_, {_, _, _, Count, MaxCount, _}=Accum)
  when Count >= MaxCount ->
    {stop, Accum};
% ignore if seqnum is < min seqnum but set first seqnum if not set
seqread_fold_fun(#sblob_entry{seqnum=EntrySeqNum}, {Items, nil, _, Count, MaxCount, MinSeqNum})
  when EntrySeqNum < MinSeqNum ->
    {continue, {Items, EntrySeqNum, EntrySeqNum, Count, MaxCount, MinSeqNum}};
% ignore if seqnum is < min seqnum
seqread_fold_fun(#sblob_entry{seqnum=EntrySeqNum}, {Items, FirstSeqNum, _, Count, MaxCount, MinSeqNum})
  when EntrySeqNum < MinSeqNum ->
    {continue, {Items, FirstSeqNum, EntrySeqNum, Count, MaxCount, MinSeqNum}};
% collect and set first seqnum if not set
seqread_fold_fun(#sblob_entry{seqnum=EntrySeqNum}=Entry, {Items, nil, _, Count, MaxCount, MinSeqNum}) ->
    {continue, {[Entry|Items], EntrySeqNum, EntrySeqNum, Count + 1, MaxCount, MinSeqNum}};
% otherwise just collect
seqread_fold_fun(#sblob_entry{seqnum=EntrySeqNum}=Entry, {Items, FirstSeqNum, _, Count, MaxCount, MinSeqNum}) ->
    {continue, {[Entry|Items], FirstSeqNum, EntrySeqNum, Count + 1, MaxCount, MinSeqNum}}.

seqread(Path, ChunkName, SeqNum, Count, Opts) ->
    lager:debug("seqread ~s ~p ~p", [ChunkName, SeqNum, Count]),
    SblobPath = filename:join([Path, ChunkName]),
    ReadAhead = proplists:get_value(read_ahead, Opts, ?SBLOB_DEFAULT_READ_AHEAD),
    Handle = open_file(SblobPath, ReadAhead, file),
    FoldFun = fun seqread_fold_fun/2,
    {_, AccOut}  = do_fold(Handle, FoldFun, {[], nil, nil, 0, Count, SeqNum}),
    {Items, FirstSeqNum, LastSeqNum, ItemsCount, _, _} = AccOut,
    {lists:reverse(Items), FirstSeqNum, LastSeqNum, ItemsCount}.

do_fold(Handle, Fun, Acc0) ->
    case raw_get_next(Handle) of
        eof ->
            file:close(Handle),
            {eof, Acc0};

        Entry ->
            case Fun(Entry, Acc0) of
                {continue, Acc1} ->
                    do_fold(Handle, Fun, Acc1);

                {stop, _AccEnd}=Res -> 
                    file:close(Handle),
                    Res
            end
    end.

% like lists:foldl but the result of Fun is a tagged tuple that indicates
% if it should continue or stop
% Fun = fun((Elem :: T, AccIn) -> Res)
% Res = {stop, AccOut} | {continue, AccOut}
% it will return a tagged tuple with one of {stop, AccEnd} | {eof, AccEnd}
% depending on how it stopped processing
fold(Path, ChunkName, Opts, Fun, Acc0) ->
    ReadAhead = proplists:get_value(read_ahead, Opts, ?SBLOB_DEFAULT_READ_AHEAD),
    SblobPath = filename:join([Path, ChunkName]),
    Handle = open_file(SblobPath, ReadAhead, file),
    do_fold(Handle, Fun, Acc0).

% sub_file and remove_recursive adapted from 
% https://github.com/erlware/erlware_commons/blob/master/src/ec_file.erl
sub_files(From) ->
    {ok, SubFiles} = file:list_dir(From),
    [filename:join(From, SubFile) || SubFile <- SubFiles].

remove(Path) ->
    case filelib:is_dir(Path) of
        false ->
            file:delete(Path);
        true ->
            lists:foreach(fun(ChildPath) ->
                                  remove(ChildPath)
                          end, sub_files(Path)),
            file:del_dir(Path)
    end.

deep_size(Path) ->
    case filelib:is_dir(Path) of
        false ->
            filelib:file_size(Path);
        true ->
            lists:foldl(fun(ChildPath, CurSize) ->
                                  CurSize + deep_size(ChildPath)
                          end, 0, sub_files(Path))
    end.

get_blob_info(BasePath, Name, Index) ->
    FullPath = filename:join([BasePath, Name]),
    case file:read_file_info(FullPath, [{time, posix}]) of
        {ok, FileInfo} ->
            #file_info{size=Size, mtime=MTime} = FileInfo,
            #sblob_info{path=FullPath, name=Name, index=Index, size=Size, mtime=MTime};
        {error, enoent} ->
            #sblob_info{path=FullPath, name=Name, index=Index, size=0, mtime=0}
    end.

