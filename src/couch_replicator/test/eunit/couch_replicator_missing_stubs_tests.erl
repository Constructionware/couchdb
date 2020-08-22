% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_replicator_missing_stubs_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").

-define(REVS_LIMIT, 3).
-define(TIMEOUT_EUNIT, 30).

setup() ->
    couch_replicator_test_helper:create_db().

setup(remote) ->
    {remote, setup()};
setup({A, B}) ->
    Ctx = couch_replicator_test_helper:start_couch(),
    Source = setup(A),
    Target = setup(B),
    {Ctx, {Source, Target}}.

teardown({remote, DbName}) ->
    teardown(DbName);
teardown(DbName) ->
    couch_replicator_test_helper:delete_db(DbName).

teardown(_, {Ctx, {Source, Target}}) ->
    teardown(Source),
    teardown(Target),
    ok = couch_replicator_test_helper:stop_couch(Ctx).


missing_stubs_test_() ->
    Pairs = [{remote, remote}],
    {
        "Replicate docs with missing stubs (COUCHDB-1365)",
        {
            foreachx,
            fun setup/1, fun teardown/2,
            [{Pair, fun should_replicate_docs_with_missed_att_stubs/2}
             || Pair <- Pairs]
        }
    }.


should_replicate_docs_with_missed_att_stubs({From, To}, {_Ctx, {Source, Target}}) ->
    {lists:flatten(io_lib:format("~p -> ~p", [From, To])),
     {inorder, [
        should_populate_source(Source),
        should_set_target_revs_limit(Target, ?REVS_LIMIT),
        should_replicate(Source, Target),
        should_compare_databases(Source, Target),
        should_update_source_docs(Source, ?REVS_LIMIT * 2),
        should_replicate(Source, Target),
        should_compare_databases(Source, Target)
     ]}}.

should_populate_source({remote, Source}) ->
    {timeout, ?TIMEOUT_EUNIT, ?_test(populate_db(Source))}.

should_replicate({remote, Source}, {remote, Target}) ->
    {timeout, ?TIMEOUT_EUNIT, ?_test(replicate(Source, Target))}.

should_set_target_revs_limit({remote, Target}, RevsLimit) ->
    ?_test(begin
        {ok, Db} =fabric2_db:open(Target, [?ADMIN_CTX]),
        ?assertEqual(ok, fabric2_db:set_revs_limit(Db, RevsLimit))
    end).

should_compare_databases({remote, Source}, {remote, Target}) ->
    {timeout, ?TIMEOUT_EUNIT, ?_test(compare_dbs(Source, Target))}.

should_update_source_docs({remote, Source}, Times) ->
    {timeout, ?TIMEOUT_EUNIT, ?_test(update_db_docs(Source, Times))}.


populate_db(DbName) ->
    AttData = crypto:strong_rand_bytes(6000),
    Doc = #doc{
        id = <<"doc1">>,
        atts = [
            couch_att:new([
                {name, <<"doc1_att1">>},
                {type, <<"application/foobar">>},
                {att_len, byte_size(AttData)},
                {data, AttData}
           ])
        ]
    },
    couch_replicator_test_helper:create_docs(DbName, [Doc]).


update_db_docs(DbName, Times) ->
    {ok, Db} = fabric2_db:open(DbName, [?ADMIN_CTX]),
    FoldFun = fun
        ({meta, _Meta}, Acc) -> {ok, Acc};
        (complete, Acc) -> {ok, Acc};
        ({row, Row}, Acc) ->
            {_, DocId} = lists:keyfind(id, 1, Row),
             ok = update_doc(DbName, DocId, Times),
             {ok, Acc}
    end,
    Opts = [{restart_tx, true}],
    {ok, _} = fabric2_db:fold_docs(Db, FoldFun, ok, Opts),
    ok.


update_doc(_DbName, _DocId, 0) ->
    ok;

update_doc(DbName, DocId, Times) ->
    {ok, Db} = fabric2_db:open(DbName, [?ADMIN_CTX]),
    {ok, Doc} = fabric2_db:open_doc(Db, DocId, []),
    #doc{revs = {Pos, [Rev | _]}} = Doc,
    Val = base64:encode(crypto:strong_rand_bytes(100)),
    Doc1 = Doc#doc{
        revs = {Pos, [Rev]},
        body = {[{<<"value">>, Val}]}
    },
    {ok, _} = fabric2_db:update_doc(Db, Doc1),
    update_doc(DbName, DocId, Times - 1).


replicate(Source, Target) ->
    Res = couch_replicator_test_helper:replicate(Source, Target),
    ?assertMatch({ok, _}, Res).


compare_dbs(Source, Target) ->
    Res = couch_replicator_test_helper:compare_dbs(Source, Target),
    ?assertEqual(ok, Res).
