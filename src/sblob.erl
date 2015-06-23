-module(sblob).

-export([open/3, close/1, delete/1, put/2, put/3, get/2, get/3, stats/1]).

-include_lib("eunit/include/eunit.hrl").
-include("sblob.hrl").

open(Path, Name, Opts) when is_binary(Path) ->
    open(binary_to_list(Path), Name, Opts);

open(Path, Name, Opts) when is_binary(Name) ->
    open(Path, binary_to_list(Name), Opts);

open(Path, Name, Opts) ->
    Config = sblob_util:parse_config(Opts),
    AbsPath = filename:absname(Path),
    FullPath = filename:join([AbsPath, Name]),

    ok = filelib:ensure_dir(FullPath),
    Sblob = #sblob{path=AbsPath, fullpath=FullPath, name=Name, config=Config},

    Result = sblob_util:fill_bounds(Sblob),
    lager:debug("open ~p", [lager:pr(Result, ?MODULE)]),
    Result.

close(#sblob{handle=nil}=Sblob) ->
    lager:debug("close, no handle ~p", [lager:pr(Sblob, ?MODULE)]),
    Sblob;
close(#sblob{handle=Handle}=Sblob) ->
    lager:debug("close ~p", [lager:pr(Sblob, ?MODULE)]),
    case file_handle_cache:close(Handle) of
        ok -> ok;
        {error, einval} ->
            lager:warning("closing invalid file handle? ~p ~p", [Handle, lager:pr(Sblob, ?MODULE)])
    end,
    Sblob#sblob{handle=nil}.

delete(#sblob{fullpath=FullPath}=Sblob) ->
    lager:debug("delete ~p", [lager:pr(Sblob, ?MODULE)]),
    NewSblob = close(Sblob),
    % XXX: log remove result? (not for enoent)
    sblob_util:remove(FullPath),
    NewSblob.

put(Sblob, Data) ->
    Now = sblob_util:now(),
    put(Sblob, Now, Data).

put(#sblob{seqnum=SeqNum, index=Index, size=Size, name=Name}=Sblob, Timestamp, Data) ->
    {Handle, Sblob1} = sblob_util:get_handle(Sblob),
    NewSeqNum = SeqNum + 1,
    lager:debug("put ~s ~p ~p", [Name, NewSeqNum, Timestamp]),
    Blob = sblob_util:to_binary(Timestamp, NewSeqNum, Data),
    AllZerosBlob = <<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>,

    if Blob == AllZerosBlob ->
           lager:warning("writing corrupt entry sblob: ~p sblob1: ~p seqnum: ~p, newseqnum: ~p, timestamp ~p data ~p",
                         [lager:pr(Sblob), lager:pr(Sblob1), SeqNum, NewSeqNum, Timestamp, Data]),
           throw(corrupted_entry);
       true -> ok
    end,
    ok = file_handle_cache:append(Handle, Blob),

    EntryOffset = Size,
    NewIndex = sblob_idx:put(Index, NewSeqNum, EntryOffset),
    BlobSize = size(Blob),
    NewSize = Size + BlobSize,

    Entry = #sblob_entry{timestamp=Timestamp, seqnum=NewSeqNum, len=size(Data),
                         data=Data, size=BlobSize, offset=EntryOffset},

    Sblob2 = Sblob1#sblob{seqnum=NewSeqNum, index=NewIndex, size=NewSize},

    {Sblob2, Entry}.

get(Sblob, SeqNum) ->
    sblob_util:handle_get_one(get(Sblob, SeqNum, 1)).

get(Sblob=#sblob{base_seqnum=BaseSeqNum}, SeqNum, Count) when SeqNum =< BaseSeqNum ->
    get(Sblob, BaseSeqNum + 1, Count);
get(Sblob=#sblob{seqnum=LastSeqNum}, SeqNum, _Count) when SeqNum > LastSeqNum ->
    {Sblob, []};
get(Sblob=#sblob{fullpath=FullPath}, SeqNum, Count) ->
    lager:debug("get ~s ~p ~p", [FullPath, SeqNum, Count]),
    {OffsetSeqNum, Sblob1} = sblob_util:seek_to_seqnum(Sblob, SeqNum),
    {Sblob2, LastSeqNum, _Entries} = sblob_util:read_until(Sblob1, OffsetSeqNum, SeqNum, false),
    {Sblob3, _, Entries} = sblob_util:read_until(Sblob2, LastSeqNum, SeqNum + Count, true),
    {Sblob3, Entries}.

stats(#sblob{base_seqnum=BaseSeqNum, seqnum=SeqNum, size=Size}) ->
    Count = SeqNum - BaseSeqNum,
    #sblob_stats{first_sn=BaseSeqNum, last_sn=SeqNum, count=Count, size=Size}.
