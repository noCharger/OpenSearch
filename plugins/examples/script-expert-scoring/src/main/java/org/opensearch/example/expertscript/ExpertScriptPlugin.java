/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.example.expertscript;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.script.ScoreScript;
import org.opensearch.script.ScoreScript.LeafFactory;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptFactory;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * An example script plugin that adds a {@link ScriptEngine}
 * implementing expert scoring.
 */
public class ExpertScriptPlugin extends Plugin implements ScriptPlugin {

    /**
     * Instantiate this plugin.
     */
    public ExpertScriptPlugin() {}

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new MyExpertScriptEngine();
    }

    /** An example {@link ScriptEngine} that uses Lucene segment details to
     *  implement pure document frequency scoring. */
    // tag::expert_engine
    private static class MyExpertScriptEngine implements ScriptEngine {
        @Override
        public String getType() {
            return "expert_scripts";
        }

        @Override
        public <T> T compile(
            String scriptName,
            String scriptSource,
            ScriptContext<T> context,
            Map<String, String> params
        ) {
            if (context.equals(ScoreScript.CONTEXT) == false) {
                throw new IllegalArgumentException(getType()
                        + " scripts cannot be used for context ["
                        + context.name + "]");
            }
            // we use the script "source" as the script identifier
            if ("custom_tf".equals(scriptSource)) {
                ScoreScript.Factory factory = new PureDfFactory();
                return context.factoryClazz.cast(factory);
            }
            throw new IllegalArgumentException("Unknown script name "
                    + scriptSource);
        }

        @Override
        public void close() {
            // optionally close resources
        }

        @Override
        public Set<ScriptContext<?>> getSupportedContexts() {
            return Collections.singleton(ScoreScript.CONTEXT);
        }

        private static class PureDfFactory implements ScoreScript.Factory,
                                                      ScriptFactory {
            @Override
            public boolean isResultDeterministic() {
                // PureDfLeafFactory only uses deterministic APIs, this
                // implies the results are cacheable.
                return true;
            }

            @Override
            public LeafFactory newFactory(
                Map<String, Object> params,
                SearchLookup lookup
            ) {
                return new PureDfLeafFactory(params, lookup);
            }
        }

        private static class PureDfLeafFactory implements LeafFactory {
            private final Map<String, Object> params;
            private final SearchLookup lookup;

            private PureDfLeafFactory(
                        Map<String, Object> params, SearchLookup lookup) {

                this.params = params;
                this.lookup = lookup;
            }

            @Override
            public boolean needs_score() {
                return false;  // Return true if the script needs the score
            }

            @Override
            public ScoreScript newInstance(LeafReaderContext context)
                    throws IOException {

                Map<String, PostingsEnum> fieldPostings = new HashMap<>();
                String term = params.get("term").toString();
                for (Object fieldObj : (List<?>)params.get("fields")) {
                    String field = fieldObj.toString();
                    PostingsEnum postings = context.reader().postings(new Term(field, term));
                    fieldPostings.put(field, postings);
                }

                double multiplier = ((Number) params.get("multiplier")).doubleValue();
                double defaultValue = ((Number) params.get("default_value")).doubleValue();

                return new ScoreScript(params, lookup, context) {
                    int currentDocid = -1;
                    @Override
                    public void setDocument(int docid) {
                        currentDocid = docid;
                        for (PostingsEnum postings : fieldPostings.values()) {
                            if (postings != null && postings.docID() < docid) {
                                try {
                                    postings.advance(docid);
                                    return;
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            }
                        }
                    }

                    @Override
                    public double execute(ExplanationHolder explanation) {
                        for (Map.Entry<String, PostingsEnum> entry : fieldPostings.entrySet()) {
                            PostingsEnum postings = entry.getValue();
                            if (postings != null && postings.docID() == currentDocid) {
                                try {
                                    return multiplier * postings.freq();
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            }
                        }
                        return defaultValue;
                    }
                };
            }
        }
    }
    // end::expert_engine
}
