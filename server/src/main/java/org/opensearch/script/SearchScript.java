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

package org.opensearch.script;

import org.opensearch.common.unit.TimeValue;

import java.util.Map;

/**
 * A script used by the Ingest Script Processor.
 *
 * @opensearch.internal
 */
public abstract class SearchScript {

    public static final String[] PARAMETERS = { "query" };

    /** The context used to compile {@link SearchScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>(
        "search",
        Factory.class,
        200,
        TimeValue.timeValueMillis(0),
        ScriptCache.UNLIMITED_COMPILATION_RATE.asTuple()
    );

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    public SearchScript(Map<String, Object> params) {
        this.params = params;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    public abstract void execute(Map<String, Object> query);

    /**
     * Factory for ingest script
     *
     * @opensearch.internal
     */
    public interface Factory {
        SearchScript newInstance(Map<String, Object> params);
    }
}
