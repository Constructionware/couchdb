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

-module(couch_replicator_filtered_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_replicator/src/couch_replicator.hrl").


-define(DDOC, #{
    <<"_id">> => <<"_design/filter_ddoc">>,
    <<"filters">> => #{
        <<"testfilter">> => <<"
            function(doc, req){if (doc.class == 'mammal') return true;}
        ">>,
        <<"queryfilter">> => <<"
            function(doc, req) {
                if (doc.class && req.query.starts) {
                    return doc.class.indexOf(req.query.starts) === 0;
                }
                else {
                    return false;
                }
            }
        ">>
    },
    <<"views">> => #{
        <<"mammals">> => #{
            <<"map">> => <<"
                function(doc) {
                    if (doc.class == 'mammal') {
                        emit(doc._id, null);
                    }
                }
            ">>
        }
    }
}).


setup(_) ->
    Ctx = test_util:start_couch([fabric, couch_replicator]),
    Source = create_db(),
    create_docs(Source),
    Target = create_db(),
    {Ctx, {Source, Target}}.

teardown(_, {Ctx, {Source, Target}}) ->
    delete_db(Source),
    delete_db(Target),
    ok = application:stop(couch_replicator),
    ok = test_util:stop_couch(Ctx).

filtered_replication_test_() ->
    Pairs = [{remote, remote}],
    {
        "Filtered replication tests",
        {
            foreachx,
            fun setup/1, fun teardown/2,
            [{Pair, fun should_succeed/2} || Pair <- Pairs]
        }
    }.

query_filtered_replication_test_() ->
    Pairs = [{remote, remote}],
    {
        "Filtered with query replication tests",
        {
            foreachx,
            fun setup/1, fun teardown/2,
            [{Pair, fun should_succeed_with_query/2} || Pair <- Pairs]
        }
    }.

view_filtered_replication_test_() ->
    Pairs = [{remote, remote}],
    {
        "Filtered with a view replication tests",
        {
            foreachx,
            fun setup/1, fun teardown/2,
            [{Pair, fun should_succeed_with_view/2} || Pair <- Pairs]
        }
    }.

should_succeed({From, To}, {_Ctx, {Source, Target}}) ->
    RepObject = {[
        {<<"source">>, db_url(From, Source)},
        {<<"target">>, db_url(To, Target)},
        {<<"filter">>, <<"filter_ddoc/testfilter">>}
    ]},
    {ok, _} = couch_replicator:replicate(RepObject, ?ADMIN_USER),
    %% FilteredFun is an Erlang version of following JS function
    %% function(doc, req){if (doc.class == 'mammal') return true;}
    FilterFun = fun(_DocId, {Props}) ->
        couch_util:get_value(<<"class">>, Props) == <<"mammal">>
    end,
    {ok, TargetDbInfo, AllReplies} = compare_dbs(Source, Target, FilterFun),
    {lists:flatten(io_lib:format("~p -> ~p", [From, To])), [
        {"Target DB has proper number of docs",
        ?_assertEqual(1, proplists:get_value(doc_count, TargetDbInfo))},
        {"Target DB doesn't have deleted docs",
        ?_assertEqual(0, proplists:get_value(doc_del_count, TargetDbInfo))},
        {"All the docs filtered as expected",
        ?_assert(lists:all(fun(Valid) -> Valid end, AllReplies))}
    ]}.

should_succeed_with_query({From, To}, {_Ctx, {Source, Target}}) ->
    RepObject = {[
        {<<"source">>, db_url(From, Source)},
        {<<"target">>, db_url(To, Target)},
        {<<"filter">>, <<"filter_ddoc/queryfilter">>},
        {<<"query_params">>, {[
            {<<"starts">>, <<"a">>}
        ]}}
    ]},
    {ok, _} = couch_replicator:replicate(RepObject, ?ADMIN_USER),
    FilterFun = fun(_DocId, {Props}) ->
        case couch_util:get_value(<<"class">>, Props) of
            <<"a", _/binary>> -> true;
            _ -> false
        end
    end,
    {ok, TargetDbInfo, AllReplies} = compare_dbs(Source, Target, FilterFun),
    {lists:flatten(io_lib:format("~p -> ~p", [From, To])), [
        {"Target DB has proper number of docs",
        ?_assertEqual(2, proplists:get_value(doc_count, TargetDbInfo))},
        {"Target DB doesn't have deleted docs",
        ?_assertEqual(0, proplists:get_value(doc_del_count, TargetDbInfo))},
        {"All the docs filtered as expected",
        ?_assert(lists:all(fun(Valid) -> Valid end, AllReplies))}
    ]}.

should_succeed_with_view({From, To}, {_Ctx, {Source, Target}}) ->
    RepObject = {[
        {<<"source">>, db_url(From, Source)},
        {<<"target">>, db_url(To, Target)},
        {<<"filter">>, <<"_view">>},
        {<<"query_params">>, {[
            {<<"view">>, <<"filter_ddoc/mammals">>}
        ]}}
    ]},
    {ok, _} = couch_replicator:replicate(RepObject, ?ADMIN_USER),
    FilterFun = fun(_DocId, {Props}) ->
        couch_util:get_value(<<"class">>, Props) == <<"mammal">>
    end,
    {ok, TargetDbInfo, AllReplies} = compare_dbs(Source, Target, FilterFun),
    {lists:flatten(io_lib:format("~p -> ~p", [From, To])), [
        {"Target DB has proper number of docs",
        ?_assertEqual(1, proplists:get_value(doc_count, TargetDbInfo))},
        {"Target DB doesn't have deleted docs",
        ?_assertEqual(0, proplists:get_value(doc_del_count, TargetDbInfo))},
        {"All the docs filtered as expected",
        ?_assert(lists:all(fun(Valid) -> Valid end, AllReplies))}
    ]}.


compare_dbs(Source, Target, FilterFun) ->
    {ok, TargetDbInfo} = fabirc2_db:get_db_info(Target),
    Fun = fun(SrcDoc, TgtDoc, Acc) ->
        case FilterFun(SrcDoc#doc.id, SrcDoc#doc.body) of
            true -> [SrcDoc == TgtDoc | Acc];
            false -> [not_found == TgtDoc | Acc]
        end
    end,
    Res = couch_replicator_test_helper:compare_fold(Source, Target, Fun, []),
    {ok, TargetDbInfo, Res}.


create_db() ->
    {ok, Db} = fabric2_db:create(?tempdb(), [?ADMIN_CTX]),
    fabric2_db:name(Db).


create_docs(DbName) ->
    couch_replicator_test_helper:create_docs(DbName, [
        ?DDOC,
        #{<<"_id">> => <<"doc1">>, <<"class">> => <<"mammal">>, <<"value">> => 1},
        #{<<"_id">> => <<"doc2">>, <<"class">> => <<"amphibians">>, <<"value">> => 2},
        #{<<"_id">> => <<"doc3">>, <<"class">> => <<"reptiles">>, <<"value">> => 3},
        #{<<"_id">> => <<"doc4">>, <<"class">> => <<"arthropods">>, <<"value">> => 2}
    ]).


delete_db(DbName) ->
    ok = fabric2_db:delete(DbName, [?ADMIN_CTX]).


db_url(remote, DbName) ->
     couch_replicator_test_helper:db_url(DbName).
