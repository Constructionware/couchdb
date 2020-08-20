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

-module(couch_replicator_create_target_with_options_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_replicator/src/couch_replicator.hrl").


setup() ->
    Ctx = couch_replicator_test_helper:start_couch(),
    Source = ?tempdb(),
    Target = ?tempdb(),
    {Ctx, {Source, Target}}.


teardown({Ctx, {Source, Target}}) ->
    delete_db(Source),
    delete_db(Target),
    ok = couch_replicator_test_helper:stop_couch(Ctx),
    ok = test_util:stop_couch(Ctx).


create_target_with_options_replication_test_() ->
    {
        "Create target with range partitions tests",
        {
            foreach,
            fun setup/0, fun teardown/1,
            [
                fun should_create_target_with_q_4/1,
                fun should_create_target_with_q_2_n_1/1,
                fun should_create_target_with_default/1,
                fun should_not_create_target_with_q_any/1
            ]
        }
    }.


should_create_target_with_q_4({_Ctx, {Source, Target}}) ->
    RepObject = {[
        {<<"source">>, Source},
        {<<"target">>, Target},
        {<<"create_target">>, true},
        {<<"create_target_params">>, {[{<<"q">>, <<"4">>}]}}
    ]},
    create_db(Source),
    create_doc(Source),
    {ok, _} = couch_replicator_test_helper:replicate(RepObject),

    TargetInfo = db_info(Target),
    {ClusterInfo} = couch_util:get_value(cluster, TargetInfo),
    delete_db(Source),
    delete_db(Target),
    ?_assertEqual(0, couch_util:get_value(q, ClusterInfo)).


should_create_target_with_q_2_n_1({_Ctx, {Source, Target}}) ->
    RepObject = {[
        {<<"source">>, Source},
        {<<"target">>, Target},
        {<<"create_target">>, true},
        {<<"create_target_params">>,
            {[{<<"q">>, <<"2">>}, {<<"n">>, <<"1">>}]}}
    ]},
    create_db(Source),
    create_doc(Source),
    {ok, _} = couch_replicator_test_helper:replicate(RepObject),

    TargetInfo = db_info(Target),
    {ClusterInfo} = couch_util:get_value(cluster, TargetInfo),
    delete_db(Source),
    delete_db(Target),
    [
        ?_assertEqual(0, couch_util:get_value(q, ClusterInfo)),
        ?_assertEqual(0, couch_util:get_value(n, ClusterInfo))
    ].


should_create_target_with_default({_Ctx, {Source, Target}}) ->
    RepObject = {[
        {<<"source">>, Source},
        {<<"target">>, Target},
        {<<"create_target">>, true}
    ]},
    create_db(Source),
    create_doc(Source),
    {ok, _} = couch_replicator_test_helper:replicate(RepObject),

    TargetInfo = db_info(Target),
    {ClusterInfo} = couch_util:get_value(cluster, TargetInfo),
    delete_db(Source),
    delete_db(Target),
    ?_assertEqual(0, couch_util:get_value(q, ClusterInfo)).


should_not_create_target_with_q_any({_Ctx, {Source, Target}}) ->
    RepObject = {[
        {<<"source">>, Source},
        {<<"target">>, Target},
        {<<"create_target">>, false},
        {<<"create_target_params">>, {[{<<"q">>, <<"1">>}]}}
    ]},
    create_db(Source),
    create_doc(Source),
    {error, _} = couch_replicator_test_helper:replicate(RepObject),
    Exists = try
        fabric2_db:open(Target, [?ADMIN_CTX]),
        ?assert(false)
    catch
        error:database_does_not_exist ->
            database_does_not_exist
    end,
    delete_db(Source),
    ?_assertEqual(Exists, database_does_not_exist).


create_doc(DbName) ->
    couch_replicator_test_helper:create_docs(DbName, [
        #{<<"_id">> => fabric2_util:uuid(), <<"foo">> => <<"bar">>}
    ]).


create_db(DbName) ->
    couch_replicator_test_helper:create_db(DbName).


delete_db(DbName) ->
    couch_replicator_test_helper:delete_db(DbName).


db_info(DbName) ->
    {ok, Db} = fabric2_db:open(DbName, [?ADMIN_CTX]),
    {ok, Info} = fabric2_db:get_db_info(Db),
    Info.
