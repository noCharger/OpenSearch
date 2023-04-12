/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.junit.Before;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.script.*;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.AbstractBuilderTestCase;

import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

import static org.opensearch.search.RandomSearchRequestGenerator.randomSearchRequest;

public class ScriptProcessorTest extends AbstractBuilderTestCase {
//    private ScriptService scriptService;
//    private Script script;
//    private SearchScript searchScript;


    // Test that the script processor can be created
    public void testScriptProcessor() throws Exception {
        String scriptName = "test-search-script";
        ScriptService scriptService = new ScriptService(
            Settings.builder().build(),
            Collections.singletonMap(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Collections.singletonMap(scriptName, ctx -> {
                    Object source = ctx.get("source");
                    if (source instanceof Map) {
                        Map<String, Object> sourceMap = (Map<String, Object>) ctx.get("source");
                        Integer size = (Integer) sourceMap.get("size");
                        sourceMap.put("size", size + 1024);
                    }
                    return null;
                }), Collections.emptyMap())
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );
        Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap());
        SearchScript searchScript = scriptService.compile(script, SearchScript.CONTEXT).newInstance(script.getParams());

        ScriptProcessor scriptProcessor = new ScriptProcessor(randomAlphaOfLength(10), null, script, null, scriptService);
        // create a random SearchRequest
        SearchRequest searchRequest = randomSearchRequest(SearchSourceBuilder::searchSource);

        scriptProcessor.processRequest(searchRequest);
        // assert size of search request
        assertEquals(1023, searchRequest.source().size());
        assertNotNull(scriptProcessor);
    }

    // Test that the script processor can modify the search request query parameter
    public void testScriptModifyQueryParameter() throws Exception {
        String scriptName = "test-search-script";
        ScriptService scriptService = new ScriptService(
            Settings.builder().build(),
            Collections.singletonMap(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Collections.singletonMap(scriptName, ctx -> {
                    Object source = ctx.get("source");
                    if (source instanceof Map) {
                        Map<String, Object> sourceMap = (Map<String, Object>) ctx.get("source");
                        Integer size = (Integer) sourceMap.get("size");
                        String query = (String) sourceMap.get("query");
                        if (query.contains("field") && size < 2) {
                            sourceMap.put("size", 1024);
                        }
                    }
                    return null;
                }), Collections.emptyMap())
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );
        Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap());
        SearchScript searchScript = scriptService.compile(script, SearchScript.CONTEXT).newInstance(script.getParams());

        ScriptProcessor scriptProcessor = new ScriptProcessor(randomAlphaOfLength(10), null, script, null, scriptService);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("field", "foo"));
        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder);
        // add a query to the search request
        scriptProcessor.processRequest(searchRequest);
        MatchQueryBuilder matchQueryBuilder = (MatchQueryBuilder) searchRequest.source().query();
        assertEquals(1024, searchRequest.source().size());
        assertNotNull(scriptProcessor);
    }
}
