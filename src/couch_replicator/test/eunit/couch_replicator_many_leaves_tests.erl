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

-module(couch_replicator_many_leaves_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").

-define(DOCS_CONFLICTS, [
    {<<"doc1">>, 10},
    % use some _design docs as well to test the special handling for them
    {<<"_design/doc2">>, 100},
    % a number > MaxURLlength (7000) / length(DocRevisionString)
    {<<"doc3">>, 210} %210}
]).
-define(NUM_ATTS, 2).
-define(TIMEOUT_EUNIT, 60).
-define(i2l(I), integer_to_list(I)).
-define(io2b(Io), iolist_to_binary(Io)).

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


docs_with_many_leaves_test_() ->
    Pairs = [{remote, remote}],
    {
        "Replicate documents with many leaves",
        {
            foreachx,
            fun setup/1, fun teardown/2,
            [{Pair, fun should_populate_replicate_compact/2}
             || Pair <- Pairs]
        }
    }.


should_populate_replicate_compact({From, To}, {_Ctx, {Source, Target}}) ->
    {lists:flatten(io_lib:format("~p -> ~p", [From, To])),
     {inorder, [
        should_populate_source(Source),
        should_replicate(Source, Target),
        should_verify_target(Source, Target),
        should_add_attachments_to_source(Source),
        should_replicate(Source, Target),
        should_verify_target(Source, Target)
     ]}}.


should_populate_source({remote, Source}) ->
    {timeout, ?TIMEOUT_EUNIT, ?_test(populate_db(Source))}.

should_replicate({remote, Source}, {remote, Target}) ->
    {timeout, ?TIMEOUT_EUNIT, ?_test(replicate(Source, Target))}.

should_verify_target({remote, Source}, {remote, Target}) ->
    {timeout, ?TIMEOUT_EUNIT, ?_test(begin
        {ok, SourceDb} = fabric2_db:open(Source, [?ADMIN_CTX]),
        {ok, TargetDb} = fabric2_db:open(Target, [?ADMIN_CTX]),
        verify_target(SourceDb, TargetDb, ?DOCS_CONFLICTS)
    end)}.

should_add_attachments_to_source({remote, Source}) ->
    {timeout, ?TIMEOUT_EUNIT, ?_test(begin
        {ok, SourceDb} = fabric2_db:open(Source, [?ADMIN_CTX]),
        add_attachments(SourceDb, ?NUM_ATTS, ?DOCS_CONFLICTS)
    end)}.

populate_db(DbName) ->
    {ok, Db} = fabric2_db:open(DbName, [?ADMIN_CTX]),
    lists:foreach(
       fun({DocId, NumConflicts}) ->
            Value = <<"0">>,
            Doc = #doc{
                id = DocId,
                body = {[ {<<"value">>, Value} ]}
            },
            {ok, _} = fabric2_db:update_doc(Db, Doc),
            {ok, _} = add_doc_siblings(Db, DocId, NumConflicts)
        end, ?DOCS_CONFLICTS).


add_doc_siblings(#{} = Db, DocId, NumLeaves) when NumLeaves > 0 ->
    add_doc_siblings(Db, DocId, NumLeaves, [], []).

add_doc_siblings(#{} = Db, _DocId, 0, AccDocs, AccRevs) ->
    {ok, []} = fabric2_db:update_docs(Db, AccDocs, [replicated_changes]),
    {ok, AccRevs};

add_doc_siblings(#{} = Db, DocId, NumLeaves, AccDocs, AccRevs) ->
    Value = ?l2b(?i2l(NumLeaves)),
    Rev = couch_hash:md5_hash(Value),
    Doc = #doc{
        id = DocId,
        revs = {1, [Rev]},
        body = {[ {<<"value">>, Value} ]}
    },
    add_doc_siblings(Db, DocId, NumLeaves - 1,
                     [Doc | AccDocs], [{1, Rev} | AccRevs]).

verify_target(_SourceDb, _TargetDb, []) ->
    ok;
verify_target(#{} = SourceDb, #{} = TargetDb, [{DocId, NumConflicts} | Rest]) ->
    Opts = [conflicts, deleted_conflicts],
    {ok, SourceLookups} = open_doc_revs(SourceDb, DocId, Opts),
    {ok, TargetLookups} = open_doc_revs(TargetDb, DocId, Opts),
    SourceDocs = [Doc || {ok, Doc} <- SourceLookups],
    TargetDocs = [Doc || {ok, Doc} <- TargetLookups],
    Total = NumConflicts + 1,
    ?assertEqual(Total, length(TargetDocs)),
    lists:foreach(
        fun({SourceDoc, TargetDoc}) ->
            ?assertEqual(json_doc(SourceDoc), json_doc(TargetDoc))
        end,
        lists:zip(SourceDocs, TargetDocs)),
    verify_target(SourceDb, TargetDb, Rest).

add_attachments(_SourceDb, _NumAtts,  []) ->
    ok;
add_attachments(#{} = SourceDb, NumAtts,  [{DocId, NumConflicts} | Rest]) ->
    {ok, SourceLookups} = open_doc_revs(SourceDb, DocId, []),
    SourceDocs = [Doc || {ok, Doc} <- SourceLookups],
    Total = NumConflicts + 1,
    ?assertEqual(Total, length(SourceDocs)),
    NewDocs = lists:foldl(
        fun(#doc{atts = Atts, revs = {Pos, [Rev | _]}} = Doc, Acc) ->
            NewAtts = lists:foldl(fun(I, AttAcc) ->
                [att(I, {Pos, Rev}, 100) | AttAcc]
            end, [], lists:seq(1, NumAtts)),
            [Doc#doc{atts = Atts ++ NewAtts} | Acc]
        end,
        [], SourceDocs),
    lists:foreach(fun(#doc{} = Doc) ->
        ?assertMatch({ok, _}, fabric2_db:update_doc(SourceDb, Doc))
    end, NewDocs),
    add_attachments(SourceDb, NumAtts, Rest).


att(I, PosRev, Size) ->
    Name =  ?io2b(["att_", ?i2l(I), "_", couch_doc:rev_to_str(PosRev)]),
    AttData = crypto:strong_rand_bytes(Size),
    couch_att:new([
        {name, Name},
        {type, <<"application/foobar">>},
        {att_len, byte_size(AttData)},
        {data, AttData}
    ]).


open_doc_revs(#{} = Db, DocId, Opts) ->
    fabric2_db:open_doc_revs(Db, DocId, all, Opts).


json_doc(#doc{} = Doc) ->
    couch_doc:to_json_obj(Doc, [attachments]).


replicate(Source, Target) ->
    couch_replicator_test_helper:replicate(Source, Target).
