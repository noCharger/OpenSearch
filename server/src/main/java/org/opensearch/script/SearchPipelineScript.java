/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.script;

import org.opensearch.common.unit.TimeValue;

import java.util.Map;

/**
 * A script used by the Ingest Script Processor.
 *
 * @opensearch.internal
 */
public abstract class SearchPipelineScript {

    public static final String[] PARAMETERS = { "ctx" };

    /** The context used to compile {@link SearchPipelineScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>(
        "search",
        Factory.class,
        200,
        TimeValue.timeValueMillis(0),
        ScriptCache.UNLIMITED_COMPILATION_RATE.asTuple()
    );

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    public SearchPipelineScript(Map<String, Object> params) {
        this.params = params;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    public abstract void execute(Map<String, Object> ctx);

    /**
     * Factory for ingest script
     *
     * @opensearch.internal
     */
    public interface Factory {
        SearchPipelineScript newInstance(Map<String, Object> params);
    }
}
