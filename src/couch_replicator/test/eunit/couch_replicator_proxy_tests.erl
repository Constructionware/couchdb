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

-module(couch_replicator_proxy_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch_replicator/src/couch_replicator.hrl").
%-include_lib("couch_replicator/include/couch_replicator_api_wrap.hrl").


setup() ->
    ok.


teardown(_) ->
    ok.


replicator_proxy_test_() ->
    {
        "replicator proxy tests",
        {
            setup,
            fun couch_replicator_test_helper:start_couch/0,
            fun couch_replicator_test_helper:stop_couch/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun parse_rep_doc_without_proxy/1,
                    fun parse_rep_doc_with_proxy/1,
                    fun parse_rep_source_target_proxy/1,
                    fun mutually_exclusive_proxy_and_source_proxy/1,
                    fun mutually_exclusive_proxy_and_target_proxy/1
                ]
            }
        }
    }.


parse_rep_doc_without_proxy(_) ->
    ?_test(begin
        NoProxyDoc = {[
            {<<"source">>, <<"http://unproxied.com">>},
            {<<"target">>, <<"http://otherunproxied.com">>}
        ]},
        Rep = couch_replicator_parse:parse_rep_doc(NoProxyDoc),
        Src = maps:get(?SOURCE, Rep),
        Tgt = maps:get(?TARGET, Rep),
        ?assertEqual(null, maps:get(<<"proxy_url">>, Src)),
        ?assertEqual(null, maps:get(<<"proxy_url">>, Tgt))
    end).


parse_rep_doc_with_proxy(_) ->
    ?_test(begin
        ProxyURL = <<"http://myproxy.com">>,
        ProxyDoc = {[
            {<<"source">>, <<"http://unproxied.com">>},
            {<<"target">>, <<"http://otherunproxied.com">>},
            {<<"proxy">>, ProxyURL}
        ]},
        Rep = couch_replicator_parse:parse_rep_doc(ProxyDoc),
        Src = maps:get(?SOURCE, Rep),
        Tgt = maps:get(?TARGET, Rep),
        ?assertEqual(ProxyURL, maps:get(<<"proxy_url">>, Src)),
        ?assertEqual(ProxyURL, maps:get(<<"proxy_url">>, Tgt))
    end).


parse_rep_source_target_proxy(_) ->
    ?_test(begin
        SrcProxyURL = <<"http://mysrcproxy.com">>,
        TgtProxyURL = <<"http://mytgtproxy.com:9999">>,
        ProxyDoc = {[
            {<<"source">>, <<"http://unproxied.com">>},
            {<<"target">>, <<"http://otherunproxied.com">>},
            {<<"source_proxy">>, SrcProxyURL},
            {<<"target_proxy">>, TgtProxyURL}
        ]},
        Rep = couch_replicator_parse:parse_rep_doc(ProxyDoc),
        Src = maps:get(?SOURCE, Rep),
        Tgt = maps:get(?TARGET, Rep),
        ?assertEqual(SrcProxyURL, maps:get(<<"proxy_url">>, Src)),
        ?assertEqual(TgtProxyURL, maps:get(<<"proxy_url">>, Tgt))
    end).


mutually_exclusive_proxy_and_source_proxy(_) ->
    ?_test(begin
        ProxyDoc = {[
            {<<"source">>, <<"http://unproxied.com">>},
            {<<"target">>, <<"http://otherunproxied.com">>},
            {<<"proxy">>, <<"oldstyleproxy.local">>},
            {<<"source_proxy">>, <<"sourceproxy.local">>}
        ]},
        ?assertThrow({bad_rep_doc, _},
            couch_replicator_parse:parse_rep_doc(ProxyDoc))
    end).


mutually_exclusive_proxy_and_target_proxy(_) ->
    ?_test(begin
        ProxyDoc = {[
            {<<"source">>, <<"http://unproxied.com">>},
            {<<"target">>, <<"http://otherunproxied.com">>},
            {<<"proxy">>, <<"oldstyleproxy.local">>},
            {<<"target_proxy">>, <<"targetproxy.local">>}
        ]},
        ?assertThrow({bad_rep_doc, _},
            couch_replicator_parse:parse_rep_doc(ProxyDoc))
    end).
