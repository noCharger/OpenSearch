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

package org.opensearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.valuesource.SumTotalTermFreqValueSource;
import org.apache.lucene.queries.function.valuesource.TFValueSource;
import org.apache.lucene.queries.function.valuesource.TermFreqValueSource;
import org.apache.lucene.queries.function.valuesource.TotalTermFreqValueSource;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.common.lucene.BytesRefs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Per-segment version of {@link SearchLookup}.
 *
 * @opensearch.internal
 */
public class LeafSearchLookup {

    final LeafReaderContext ctx;
    final LeafDocLookup docMap;
    final SourceLookup sourceLookup;
    final LeafFieldsLookup fieldsLookup;
    final Map<String, Object> asMap;

    public LeafSearchLookup(LeafReaderContext ctx, LeafDocLookup docMap, SourceLookup sourceLookup, LeafFieldsLookup fieldsLookup) {
        this.ctx = ctx;
        this.docMap = docMap;
        this.sourceLookup = sourceLookup;
        this.fieldsLookup = fieldsLookup;

        Map<String, Object> asMap = new HashMap<>(4);
        asMap.put("doc", docMap);
        asMap.put("_doc", docMap);
        asMap.put("_source", sourceLookup);
        asMap.put("_fields", fieldsLookup);
        this.asMap = unmodifiableMap(asMap);
    }

    public Map<String, Object> asMap() {
        return this.asMap;
    }

    public SourceLookup source() {
        return this.sourceLookup;
    }

    public LeafFieldsLookup fields() {
        return this.fieldsLookup;
    }

    public LeafDocLookup doc() {
        return this.docMap;
    }

    public void setDocument(int docId) {
        docMap.setDocument(docId);
        sourceLookup.setSegmentAndDocument(ctx, docId);
        fieldsLookup.setDocument(docId);
    }

    public int termFreq(String field, String term, int docId) throws IOException {
        TermFreqValueSource valueSource = new TermFreqValueSource(field, term, field, BytesRefs.toBytesRef(term));
        return valueSource.getValues(null, ctx).intVal(docId);
    }

    public float tf(String field, String term, int docId, IndexSearcher indexSearcher) throws IOException {
        TFValueSource valueSource = new TFValueSource(field, term, field, BytesRefs.toBytesRef(term));
        Map context = new HashMap();
        context.put("searcher", indexSearcher);
        return valueSource.getValues(context, ctx).floatVal(docId);
    }

    public long totalTermFreq(String field, String term, int docId, IndexSearcher indexSearcher) throws IOException {
        TotalTermFreqValueSource valueSource = new TotalTermFreqValueSource(field, term, field, BytesRefs.toBytesRef(term));
        Map context = new HashMap();
        valueSource.createWeight(context, indexSearcher);
        return valueSource.getValues(context, ctx).longVal(docId);
    }

    public long sumTotalTermFreq(String field, int docId, IndexSearcher indexSearcher) throws IOException {
        SumTotalTermFreqValueSource valueSource = new SumTotalTermFreqValueSource(field);
        Map context = new HashMap();
        valueSource.createWeight(context, indexSearcher);
        return valueSource.getValues(context, ctx).longVal(docId);
    }
}
