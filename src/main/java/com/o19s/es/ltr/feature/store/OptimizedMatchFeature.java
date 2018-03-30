/*
 * Copyright [2017] Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.o19s.es.ltr.feature.store;

import com.o19s.es.ltr.feature.Feature;
import com.o19s.es.ltr.feature.FeatureSet;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.search.MatchQuery;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;

public class OptimizedMatchFeature extends QueryBuilder implements Feature, Accountable {
    private static final long BASE_RAM_USED = RamUsageEstimator.shallowSizeOfInstance(StoredFeature.class);
    public static final String CACHED_TOKEN_STREAMS_KEY = "_ltrcachedtokenstreams_";

    private final String name;
    private final String field;
    private final String analyzer;
    private final String minimumShouldMatch;
    private final String queryParam;
    private final Operator defaultOperator;


    public OptimizedMatchFeature(String name, String field, String analyzer, String minimumShouldMatch, String queryParam,
                                 Operator defaultOperator) {
        // we don't know it yet but we don't use the default one provided anyways
        super(null);
        this.name = Objects.requireNonNull(name);
        this.field = Objects.requireNonNull(field);
        this.analyzer = analyzer;
        this.minimumShouldMatch = minimumShouldMatch;
        this.queryParam = Objects.requireNonNull(queryParam);
        this.defaultOperator = Objects.requireNonNull(defaultOperator);
    }

    /**
     * The feature name
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * Transform this feature into a lucene query
     */
    @Override
    public Query doToQuery(QueryShardContext context, FeatureSet set, Map<String, Object> params) {
        NamedAnalyzer analyzer;
        CachedTokenStreams cache = (CachedTokenStreams) params.computeIfAbsent(CACHED_TOKEN_STREAMS_KEY,
                (k) -> new CachedTokenStreams());

        if (this.analyzer != null) {
            analyzer = context.getMapperService().getIndexAnalyzers().get(this.analyzer);
            if (analyzer == null) {
                throw new IllegalArgumentException("No analyzer found for [" + this.analyzer + "]");
            }
        } else {
            MappedFieldType type = context.fieldMapper(this.field);
            if (type == null) {
                return Queries.newUnmappedFieldQuery(this.field);
            }
            if (type.searchAnalyzer() != null) {
                analyzer = type.searchAnalyzer();
            } else {
                Analyzer mainAnalyzer = context.getMapperService().searchAnalyzer();
                analyzer = new NamedAnalyzer( "_ltr_mapper_default_", AnalyzerScope.INDEX, mainAnalyzer);
            }
        }

        MatchQuery query = new MatchQuery(context);
        Object oquery = params.get(this.queryParam);

        if (oquery == null) {
            throw new IllegalArgumentException("Missing required param: [" + queryParam + "]");
        }
        if (!(oquery instanceof String)) {
            throw new IllegalArgumentException("Required param: [" + queryParam + "] must be a String but " +
                    "received a [" + oquery.getClass().getSimpleName() + "]");
        }
        String queryString = (String) oquery;
        query.setAnalyzer(cache.getAnalyzer(analyzer, queryString));
        query.setOccur(this.defaultOperator.toBooleanClauseOccur());
        try {
            return query.parse(MatchQuery.Type.BOOLEAN, field, queryString);
        } catch (IOException e) {
            throw new QueryShardException(context, "Cannot create optimized match query while parsing feature [" + this.field + "]", e);
        }
    }

    /**
     * Return the memory usage of this object in bytes. Negative values are illegal.
     */
    @Override
    public long ramBytesUsed() {
        return BASE_RAM_USED +
                (Character.BYTES * name.length()) + NUM_BYTES_ARRAY_HEADER +
                (Character.BYTES * field.length()) + NUM_BYTES_ARRAY_HEADER +
                (Character.BYTES * queryParam.length()) + NUM_BYTES_ARRAY_HEADER +
                (analyzer != null ?
                    (Character.BYTES * analyzer.length()) + NUM_BYTES_ARRAY_HEADER : 0) +
                (minimumShouldMatch != null ?
                    (Character.BYTES * minimumShouldMatch.length()) + NUM_BYTES_ARRAY_HEADER : 0);
    }
}
