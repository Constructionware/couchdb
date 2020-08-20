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

-module(couch_replicator_id_too_long_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_replicator/src/couch_replicator.hrl").


setup(_) ->
    Ctx = couch_replicator_test_helper:start_couch(),
    Source = couch_replicator_test_helper:create_db(),
    create_doc(Source),
    Target = couch_replicator_test_helper:create_db(),
    {Ctx, {Source, Target}}.


teardown(_, {Ctx, {Source, Target}}) ->
    couch_replicator_test_helper:delete_db(Source),
    couch_replicator_test_helper:delete_db(Target),
    config:delete("replicator", "max_document_id_length"),
    ok = couch_replicator_test_helper:stop_couch(Ctx).


id_too_long_replication_test_() ->
    Pairs = [{remote, remote}],
    {
        "Doc id too long tests",
        {
            foreachx,
            fun setup/1, fun teardown/2,
            [{Pair, fun should_succeed/2} || Pair <- Pairs] ++
            [{Pair, fun should_fail/2} || Pair <- Pairs]
        }
    }.


should_succeed({From, To}, {_Ctx, {Source, Target}}) ->
    RepObject = {[
        {<<"source">>, db_url(From, Source)},
        {<<"target">>, db_url(To, Target)}
    ]},
    config:set("replicator", "max_document_id_length", "5", _Persist = false),
    {ok, _} = couch_replicator_test_helper:replicate(RepObject),
    ?_assertEqual(ok, couch_replicator_test_helper:compare_dbs(Source, Target)).


should_fail({From, To}, {_Ctx, {Source, Target}}) ->
    RepObject = {[
        {<<"source">>, db_url(From, Source)},
        {<<"target">>, db_url(To, Target)}
    ]},
    config:set("replicator", "max_document_id_length", "4", _Persist = false),
    {ok, _} = couch_replicator_test_helper:replicate(RepObject),
    ExceptIds = [<<"12345">>],
    ?_assertEqual(ok, couch_replicator_test_helper:compare_dbs(Source, Target,
        ExceptIds)).


create_doc(DbName) ->
    Docs = [#{<<"_id">> => <<"12345">>}],
    couch_replicator_test_helper:create_docs(DbName, Docs).


db_url(remote, DbName) ->
    couch_replicator_test_helper:db_url(DbName).
