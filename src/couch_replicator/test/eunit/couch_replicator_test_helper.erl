-module(couch_replicator_test_helper).


-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_replicator/src/couch_replicator.hrl").


-export([
    create_docs/2,
    compare_dbs/2,
    compare_dbs/3,
    compare_docs/2,
    db_url/1,
    replicate/1,
    get_pid/1,
    replicate/2
]).


create_docs(DbName, Docs) when is_binary(DbName), is_list(Docs) ->
    {ok, Db} = fabric2_db:open(DbName, [?ADMIN_CTX]),
    Docs1 = lists:map(fun(Doc) ->
        case Doc of
            #{} ->
                Doc1 = couch_util:json_decode(couch_util:json_encode(Doc)),
                couch_doc:from_json_obj(Doc1);
            #doc{} ->
                Doc
        end
    end, Docs),
    {ok, ResList} = fabric2_db:update_docs(Db, Docs1),
    lists:foreach(fun(Res) ->
        ?assertMatch({ok, {_, Rev}} when is_binary(Rev), Res)
    end, ResList).


compare_dbs(Source, Target) ->
    Fun = fun(SrcDoc, TgtDoc, ok) -> compare_docs(SrcDoc, TgtDoc) end,
    compare_fold(Source, Target, Fun, ok).


compare_dbs(Source, Target, ExceptIds) when is_binary(Source),
        is_binary(Target), is_list(ExceptIds) ->
    Fun = fun(SrcDoc, TgtDoc, ok) ->
        case lists:memeber(SrcDoc#doc.id, ExceptIds) of
            true -> ?assertEqual(not_found, TgtDoc);
            false -> compare_docs(SrcDoc, TgtDoc)
        end,
        ok
    end,
    compare_fold(Source, Target, Fun, ok).


compare_fold(Source, Target, Fun, Acc0) when
        is_binary(Source), is_binary(Target), is_function(Fun, 3) ->
    {ok, SourceDb} = fabric2_db:open(Source, [?ADMIN_CTX]),
    {ok, TargetDb} = fabric2_db:open(Target, [?ADMIN_CTX]),
    fabric2_fdb:transactional(SourceDb, fun(TxSourceDb) ->
        FoldFun = fun
            ({meta, _Meta}, Acc) -> {ok, Acc};
            (complete, Acc) -> {ok, Acc};
            ({row, Row}, Acc) ->
                {_, Id} = lists:keyfind(id, 1, Row),
                SrcDoc = open_doc(TxSourceDb, Id),
                TgtDoc = open_doc(TargetDb, Id),
                {ok, Fun(SrcDoc, TgtDoc, Acc)}
        end,
        Opts = [{restart_tx, true}],
        {ok, AccF} = fabric2_db:fold_docs(TxSourceDb, FoldFun, Acc0, Opts),
        AccF
    end).


open_doc(Db, DocId) ->
    case fabric2_db:open_doc(Db, DocId, []) of
        {ok, #doc{deleted = false} = Doc} -> Doc;
        {not_found, missing} -> not_found
    end.


compare_docs(#doc{} = Doc1, Doc2) when
        is_record(Doc2, doc) orelse Doc2 =:= not_found ->
    ?assert(Doc2 =/= not_found),
    ?assertEqual(Doc1#doc.body, Doc2#doc.body),
    #doc{atts = Atts1} = Doc1,
    #doc{atts = Atts2} = Doc2,
    ?assertEqual(lists:sort([couch_att:fetch(name, Att) || Att <- Atts1]),
                 lists:sort([couch_att:fetch(name, Att) || Att <- Atts2])),
    FunCompareAtts = fun(Att) ->
        AttName = couch_att:fetch(name, Att),
        {ok, AttTarget} = find_att(Atts2, AttName),
        SourceMd5 = att_md5(Att),
        TargetMd5 = att_md5(AttTarget),
        case AttName of
            <<"att1">> ->
                ?assertEqual(gzip, couch_att:fetch(encoding, Att)),
                ?assertEqual(gzip, couch_att:fetch(encoding, AttTarget)),
                DecSourceMd5 = att_decoded_md5(Att),
                DecTargetMd5 = att_decoded_md5(AttTarget),
                ?assertEqual(DecSourceMd5, DecTargetMd5);
            _ ->
                ?assertEqual(identity, couch_att:fetch(encoding, AttTarget)),
                ?assertEqual(identity, couch_att:fetch(encoding, AttTarget))
        end,
        ?assertEqual(SourceMd5, TargetMd5),
        ?assert(is_integer(couch_att:fetch(disk_len, Att))),
        ?assert(is_integer(couch_att:fetch(att_len, Att))),
        ?assert(is_integer(couch_att:fetch(disk_len, AttTarget))),
        ?assert(is_integer(couch_att:fetch(att_len, AttTarget))),
        ?assertEqual(couch_att:fetch(disk_len, Att),
                     couch_att:fetch(disk_len, AttTarget)),
        ?assertEqual(couch_att:fetch(att_len, Att),
                     couch_att:fetch(att_len, AttTarget)),
        ?assertEqual(couch_att:fetch(type, Att),
                     couch_att:fetch(type, AttTarget)),
        ?assertEqual(couch_att:fetch(md5, Att),
                     couch_att:fetch(md5, AttTarget))
    end,
    lists:foreach(FunCompareAtts, Atts1).


find_att([], _Name) ->
    nil;
find_att([Att | Rest], Name) ->
    case couch_att:fetch(name, Att) of
        Name ->
            {ok, Att};
        _ ->
            find_att(Rest, Name)
    end.


att_md5(Att) ->
    Md50 = couch_att:foldl(
        Att,
        fun(Chunk, Acc) -> couch_hash:md5_hash_update(Acc, Chunk) end,
        couch_hash:md5_hash_init()),
    couch_hash:md5_hash_final(Md50).

att_decoded_md5(Att) ->
    Md50 = couch_att:foldl_decode(
        Att,
        fun(Chunk, Acc) -> couch_hash:md5_hash_update(Acc, Chunk) end,
        couch_hash:md5_hash_init()),
    couch_hash:md5_hash_final(Md50).

db_url(DbName) ->
    Addr = config:get("httpd", "bind_address", "127.0.0.1"),
    Port = mochiweb_socket_server:get(couch_httpd, port),
    ?l2b(io_lib:format("http://~s:~b/~s", [Addr, Port, DbName])).


get_pid(RepId) ->
    JobId = case couch_replicator_jobs:get_job_id(undefined, RepId) of
        {ok, JobId0} -> JobId0;
        {error, not_found} -> RepId
    end,
    {ok, #{<<"state">> := <<"running">>, <<"rep_pid">> := Pid0}} =
        couch_replicator_jobs:get_job_data(undefined, JobId),
    Pid = list_to_pid(binary_to_list(Pid0)),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    Pid.


replicate(Source, Target) ->
    replicate(#{
        <<"source">> => Source,
        <<"target">> => Target
    }).


replicate(Rep) when is_tuple(Rep) orelse is_map(Rep) ->
    {ok, Id, _} = couch_replicator_parse:parse_transient_rep(Rep, ?ADMIN_USER),
    ok = cancel(Id),
    {ok, Res = #{}} = couch_replicator:replicate(Rep, ?ADMIN_CTX),
    io:format(standard_error, "~nXXXX REP RESULT :~p~n", [Res]),
    ok = cancel(Id).


replicate_continuous(Source, Target) ->
    replicate(#{
        <<"source">> => Source,
        <<"target">> => Target,
        <<"continuous">> => true
    }).


replicate_continuous(Rep) when is_tuple(Rep) orelse is_map(Rep) ->
    {ok, Id, _} = couch_replicator_parse:parse_transient_rep(Rep, ?ADMIN_USER),
    ok = cancel(Id),
    {ok, {continuous, Id}} = couch_replicator:replicate(Rep, ?ADMIN_CTX),
    {ok, get_pid(Id), Id}.


cancel(Id) when is_binary(Id) ->
    CancelRep = #{<<"cancel">> => true, <<"id">> => Id},
    case couch_replicator:replicate(CancelRep, ?ADMIN_CTX) of
        {ok, {cancelled, <<_/binary>>}} -> ok;
        {error, not_found} -> ok
    end.
