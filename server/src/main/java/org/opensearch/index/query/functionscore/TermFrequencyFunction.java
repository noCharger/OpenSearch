/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query.functionscore;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.valuesource.SumTotalTermFreqValueSource;
import org.apache.lucene.queries.function.valuesource.TFValueSource;
import org.apache.lucene.queries.function.valuesource.TermFreqValueSource;
import org.apache.lucene.queries.function.valuesource.TotalTermFreqValueSource;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.common.lucene.BytesRefs;

import java.io.IOException;
import java.util.Map;

public abstract class TermFrequencyFunction {

    protected final String field;
    protected final String term;
    protected final int docId;
    protected Map<Object, Object> context;

    public TermFrequencyFunction(String field, String term, int docId, Map<Object, Object> context) {
        this.field = field;
        this.term = term;
        this.docId = docId;
        this.context = context;
    }

    public abstract Object execute(LeafReaderContext readerContext) throws IOException;

    public static class TermFrequencyFunctionFactory {
        public static TermFrequencyFunction createFunction(String functionName, String field, String term, int docId, Map<Object, Object> context) {
            switch (functionName) {
                case "termFreq":
                    return new TermFreqFunction(field, term, docId, context);
                case "tf":
                    return new TFFunction(field, term, docId, context);
                case "totalTermFreq":
                    return new TotalTermFreq(field, term, docId, context);
                case "sumTotalTermFreq":
                    return new SumTotalTermFreq(field, term, docId, context);
                default:
                    throw new IllegalArgumentException("Unsupported function: " + functionName);
            }
        }
    }

    public static class TermFreqFunction extends TermFrequencyFunction {

        public TermFreqFunction(String field, String term, int docId, Map<Object, Object> context) {
            super(field, term, docId, context);
        }

        @Override
        public Integer execute(LeafReaderContext readerContext) throws IOException {
            TermFreqValueSource valueSource = new TermFreqValueSource(field, term, field, BytesRefs.toBytesRef(term));
            return valueSource.getValues(null, readerContext).intVal(docId);
        }
    }

    public static class TFFunction extends TermFrequencyFunction {

        public TFFunction(String field, String term, int docId, Map<Object, Object> context) {
            super(field, term, docId, context);
        }

        @Override
        public Float execute(LeafReaderContext readerContext) throws IOException {
            TFValueSource valueSource = new TFValueSource(field, term, field, BytesRefs.toBytesRef(term));
            return valueSource.getValues(context, readerContext).floatVal(docId);
        }
    }

    public static class TotalTermFreq extends TermFrequencyFunction {

        public TotalTermFreq(String field, String term, int docId, Map<Object, Object> context) {
            super(field, term, docId, context);
        }

        @Override
        public Long execute(LeafReaderContext readerContext) throws IOException {
            TotalTermFreqValueSource valueSource = new TotalTermFreqValueSource(field, term, field, BytesRefs.toBytesRef(term));
            valueSource.createWeight(context, (IndexSearcher) context.get("searcher"));
            return valueSource.getValues(context, readerContext).longVal(docId);
        }
    }

    public static class SumTotalTermFreq extends TermFrequencyFunction {

        public SumTotalTermFreq(String field, String term, int docId, Map<Object, Object> context) {
            super(field, term, docId, context);
        }

        @Override
        public Long execute(LeafReaderContext readerContext) throws IOException {
            SumTotalTermFreqValueSource valueSource = new SumTotalTermFreqValueSource(field);
            valueSource.createWeight(context, (IndexSearcher) context.get("searcher"));
            return valueSource.getValues(context, readerContext).longVal(docId);
        }
    }
}
