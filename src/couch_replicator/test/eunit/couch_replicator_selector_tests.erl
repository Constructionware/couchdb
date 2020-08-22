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

-module(couch_replicator_selector_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_replicator/src/couch_replicator.hrl").


setup(_) ->
    Ctx = couch_replicator_test_helper:start_couch(),
    Source = couch_replicator_test_helper:create_db(),
    create_docs(Source),
    Target = couch_replicator_test_helper:create_db(),
    {Ctx, {Source, Target}}.

teardown(_, {Ctx, {Source, Target}}) ->
    couch_replicator_test_helper:delete_db(Source),
    couch_replicator_test_helper:delete_db(Target),
    couch_replicator_test_helper:stop_couch(Ctx).


selector_replication_test_() ->
    Pairs = [{remote, remote}],
    {
        "Selector filtered replication tests",
        {
            foreachx,
            fun setup/1, fun teardown/2,
            [{Pair, fun should_succeed/2} || Pair <- Pairs]
        }
    }.

should_succeed({From, To}, {_Ctx, {Source, Target}}) ->
    RepObject = {[
        {<<"source">>, Source},
        {<<"target">>, Target},
        {<<"selector">>, {[{<<"_id">>, <<"doc2">>}]}}
    ]},
    {ok, _} = couch_replicator_test_helper:replicate(RepObject),
    {ok, TargetDbInfo, AllReplies} = compare_dbs(Source, Target),
    {lists:flatten(io_lib:format("~p -> ~p", [From, To])), [
        {"Target DB has proper number of docs",
        ?_assertEqual(1, proplists:get_value(doc_count, TargetDbInfo))},
        {"All the docs selected as expected",
        ?_assert(lists:all(fun(Valid) -> Valid end, AllReplies))}
    ]}.


compare_dbs(Source, Target) ->
    {ok, TargetDb} = fabric2_db:open(Target, []),
    {ok, TargetDbInfo} = fabric2_db:get_db_info(TargetDb),
    Fun = fun(SrcDoc, TgtDoc, Acc) ->
        case SrcDoc#doc.id == <<"doc2">> of
            true -> [SrcDoc#doc.body == TgtDoc#doc.body | Acc];
            false -> [not_found == TgtDoc | Acc]
        end
    end,
    Res = couch_replicator_test_helper:compare_fold(Source, Target, Fun, []),
    {ok, TargetDbInfo, Res}.


create_docs(DbName) ->
    couch_replicator_test_helper:create_docs(DbName, [
        #{<<"_id">> => <<"doc1">>},
        #{<<"_id">> => <<"doc2">>}
    ]).
