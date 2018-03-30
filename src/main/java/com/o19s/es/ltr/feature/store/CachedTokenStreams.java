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

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.index.analysis.NamedAnalyzer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CachedTokenStreams {
    private final Map<Key, CachingTokenFilter> map = new HashMap<>();
    private static final Logger LOGGER = ESLoggerFactory.getLogger(CachedTokenStreams.class);

    public CachingTokenFilter get(NamedAnalyzer analyzer, String query) {
        return map.computeIfAbsent(new Key(analyzer, query),
                (k) -> new CachingTokenFilter(analyzer.tokenStream(analyzer.name(), query)));
    }

    public Analyzer getAnalyzer(NamedAnalyzer analyzer, String query) {
        return new CachingTokenFilterWrapper(get(analyzer, query));
    }

    public void clear() {
        map.entrySet().stream().forEach((entry) -> {
            try {
                entry.getValue().close();
            } catch (IOException ioe) {
                LOGGER.warn( "Failed to close analyzer {}", entry.getKey().analyzer.name());
            }
        });
        map.clear();
    }

    public static class CachingTokenFilterWrapper extends Analyzer {
        private final CachingTokenFilter filter;
        public CachingTokenFilterWrapper(CachingTokenFilter filter) {
            super(new ReuseStrategy() {
                @Override
                public TokenStreamComponents getReusableComponents(Analyzer analyzer, String fieldName) {
                    return null;
                }
                @Override
                public void setReusableComponents(Analyzer analyzer, String fieldName, TokenStreamComponents components) {
                }
            });
            this.filter = filter;
        }

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            // Evil hack
            return new TokenStreamComponents(new Tokenizer() {
                @Override
                public boolean incrementToken()  {
                    return false;
                }
            }, filter);
        }
    }
    static class Key {
        private final NamedAnalyzer analyzer;
        private final String query;

        Key(NamedAnalyzer analyzer, String query) {
            this.analyzer = analyzer;
            this.query = query;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return Objects.equals(analyzer.name(), key.analyzer.name()) &&
                    Objects.equals(query, key.query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(analyzer.name(), query);
        }
    }
}
