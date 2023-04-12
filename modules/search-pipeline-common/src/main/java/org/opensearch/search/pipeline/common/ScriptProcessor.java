/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.action.search.SearchRequest;

import org.opensearch.common.Nullable;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.util.CollectionUtils;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;

import org.opensearch.script.*;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;

import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Processor that evaluates a script with a search request in its context
 * and then returns the modified search request.
 */
public final class ScriptProcessor extends AbstractProcessor implements SearchRequestProcessor{
    /**
     * Key to reference this processor type from a search pipeline.
     */
    public static final String TYPE = "script";

    private final Script script;
    private final ScriptService scriptService;
    private final SearchScript precompiledSearchScript;

    /**
     * Processor that evaluates a script with a search request in its context
     * @param tag The processor's tag.
     * @param description The processor's description.
     * @param script The {@link Script} to execute.
     * @param precompiledSearchScript The {@link Script} precompiled
     * @param scriptService The {@link ScriptService} used to execute the script.
     */
    ScriptProcessor(
        String tag,
        String description,
        Script script,
        @Nullable SearchScript precompiledSearchScript,
        ScriptService scriptService
    ) {
        super(tag, description);
        this.script = script;
        this.precompiledSearchScript = precompiledSearchScript;
        this.scriptService = scriptService;
    }

    /**
     * Executes the script with the search request in context.
     *
     * @param request The Search request passed into the script context under the "query" object.
     */
    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        // assert request is not null and source is not null
        if (request == null || request.source() == null) {
            throw new IllegalArgumentException("search request must not be null");
        }
        final SearchScript searchScript;
        if (precompiledSearchScript == null) {
            SearchScript.Factory factory = scriptService.compile(script, SearchScript.CONTEXT);
            searchScript = factory.newInstance(script.getParams());
        } else {
            searchScript = precompiledSearchScript;
        }
        // convert the search request to a map
        Map<String, Object> cxt = Map.of("source", request.source().toMap());
        // execute the script with the search request in context
        searchScript.execute(cxt);
        CollectionUtils.ensureNoSelfReferences(cxt, "search script");
        // assert cxt has at least one key and it contains source key
        if (cxt.isEmpty() || cxt.get("source") == null) {
            throw new IllegalArgumentException("script must have at least one key");
        }

        Object obj = cxt.get("source");
        if (obj instanceof Map<?, ?>) {
            Map<?, ?> rawMap = (Map<?, ?>) obj;
            Map<String, Object> resultMap = new LinkedHashMap<>();
            boolean valid = true;

            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                if (entry.getKey() instanceof String && entry.getValue() instanceof Object) {
                    resultMap.put((String) entry.getKey(), entry.getValue());
                } else {
                    valid = false;
                    break;
                }
            }

            if (valid) {
                SearchSourceBuilder sourceBuilder = SearchSourceBuilder.fromMap(resultMap);
                request.source(sourceBuilder);
            } else {
                // Handle invalid map
            }
        } else {
            // Handle invalid object
        }
        return request;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    Script getScript() {
        return script;
    }

    SearchScript getPrecompiledSearchScript() {
        return precompiledSearchScript;
    }

    public static final class Factory implements Processor.Factory {
        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public ScriptProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            try (
                XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent).map(config);
                InputStream stream = BytesReference.bytes(builder).streamInput();
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)
            ) {
                Script script = Script.parse(parser);

                Arrays.asList("id", "source", "inline", "lang", "params", "options").forEach(config::remove);

                // verify script is able to be compiled before successfully creating processor.
                SearchScript searchScript = null;
                try {
                    final SearchScript.Factory factory = scriptService.compile(script, SearchScript.CONTEXT);
                    if (ScriptType.INLINE.equals(script.getType())) {
                        searchScript = factory.newInstance(script.getParams());
                    }
                } catch (ScriptException e) {
                    // throw newConfigurationException(TYPE, processorTag, null, e);
                    // TODO: handle exception
                }
                return new ScriptProcessor(processorTag, description, script, searchScript, scriptService);
            }
        }
    }
}
