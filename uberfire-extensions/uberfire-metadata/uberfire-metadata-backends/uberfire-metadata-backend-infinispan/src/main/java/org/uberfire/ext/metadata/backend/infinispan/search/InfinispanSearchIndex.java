/*
 * Copyright 2018 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.uberfire.ext.metadata.backend.infinispan.search;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.Search;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uberfire.ext.metadata.backend.infinispan.provider.InfinispanContext;
import org.uberfire.ext.metadata.model.KObject;
import org.uberfire.ext.metadata.model.schema.MetaObject;
import org.uberfire.ext.metadata.search.ClusterSegment;
import org.uberfire.ext.metadata.search.DateRange;
import org.uberfire.ext.metadata.search.IOSearchService;
import org.uberfire.ext.metadata.search.SearchIndex;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.uberfire.ext.metadata.backend.infinispan.utils.AttributesUtil.toProtobufFormat;

public class InfinispanSearchIndex implements SearchIndex {

    private final InfinispanContext infinispanContext;
    private final InfinispanQueryBuilder queryBuilder;

    private Logger logger = LoggerFactory.getLogger(InfinispanSearchIndex.class);

    public InfinispanSearchIndex(InfinispanContext infinispanContext) {
        this.infinispanContext = infinispanContext;
        this.queryBuilder = new InfinispanQueryBuilder();
    }

    @Override
    public List<KObject> searchByAttrs(Map<String, ?> attrs,
                                       IOSearchService.Filter filter,
                                       ClusterSegment... clusterSegments) {

        if (clusterSegments == null || clusterSegments.length == 0) {
            return emptyList();
        }
        if (attrs == null || attrs.size() == 0) {
            return emptyList();
        }
        List<String> queries =
                buildSearchByAttrsQuery(attrs,
                                        clusterSegments);

        queries.forEach(query -> logger.info("Query: " + query.build().toString()));

        return queries.stream()
                .map(q -> q.build().list())
                .flatMap(x -> x.stream())
                .map(o -> (KObject) o)
                .filter(ko -> filter.accept(ko))
                .collect(toList());
    }

    @Override
    public int searchByAttrsHits(Map<String, ?> attrs,
                                 ClusterSegment... clusterSegments) {

        List<String> queries =
                buildSearchByAttrsQuery(attrs,
                                        clusterSegments);

        queries.forEach(query -> logger.info("Query: " + query.build().toString()));

        return queries.stream()
                .mapToInt(query -> query.build().getResultSize())
                .sum();
    }

    @Override
    public List<KObject> fullTextSearch(String term,
                                        IOSearchService.Filter filter,
                                        ClusterSegment... clusterSegments) {

        List<QueryBuilder> queries = buildFullTextSearchQueries(term,
                                                                clusterSegments);

        List<KObject> hits = queries.stream()
                .map(query -> query.build().list())
                .flatMap(x -> x.stream())
                .map(x -> (KObject) x)
                .filter(hit -> filter.accept(hit))
                .collect(toList());

        return hits;
    }

    @Override
    public int fullTextSearchHits(String term,
                                  ClusterSegment... clusterSegments) {

        List<QueryBuilder> queries = buildFullTextSearchQueries(term,
                                                                clusterSegments);

        return queries.stream()
                .mapToInt(query -> query.build().getResultSize())
                .sum();
    }

    private List<QueryBuilder> buildFullTextSearchQueries(String term,
                                                          ClusterSegment[] clusterSegments) {
        List<QueryBuilder> queries =
                Arrays.asList(clusterSegments).stream()
                        .map(clusterSegment -> initializeQuery(clusterSegment))
                        .flatMap(x -> x.stream())
                        .map(tuple -> wildcardQuery(
                                term))
                        .collect(toList());

        queries.forEach(query -> logger.info("Query: " + query.build().toString()));
        return queries;
    }

    private List<String> buildSearchByAttrsQuery(Map<String, ?> attrs,
                                                 ClusterSegment[] clusterSegments) {

        return Arrays.asList(clusterSegments).stream()
                .map(clusterSegment -> initializeQuery(clusterSegment))
                .flatMap(x -> x.stream())
                .map(tuple -> attributesQuery(attrs))
                .collect(toList());
    }

    private String attributesQuery(Map<String, ?> attrs) {

        return attrs.entrySet()
                .stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(toProtobufFormat(entry.getKey()),
                                                            entry.getValue()))
                .map(entry -> {
                    if (entry.getValue() instanceof DateRange) {
                        final Long from = ((DateRange) entry.getValue()).after().getTime();
                        final Long to = ((DateRange) entry.getValue()).before().getTime();
                        return this.queryBuilder.buildDateRangeQuery(entry.getKey(),
                                                                     from,
                                                                     to);
                    } else if (entry.getValue() instanceof String) {
                        return this.queryBuilder.buildWildcardQuery(entry.getKey(),
                                                                    String.valueOf(entry.getValue()));
                    } else if (entry.getValue() instanceof Boolean) {
                        return this.queryBuilder.buildBooleanQuery(entry.getKey(),
                                                                   (Boolean) entry.getValue());
                    } else {
                        return null;
                    }
                })
                .filter(x -> x != null)
                .collect(joining(" AND "));
    }

    private String wildcardQuery(String term) {
        return this.queryBuilder.buildFullTextTermQuery(term);
    }

    private List<String> initializeQuery(ClusterSegment clusterSegment) {

        String index = clusterSegment.getClusterId();
        QueryFactory factory = this.getQueryFactory(index);
        List<String> types = this.infinispanContext.getTypes(index);

        return types.stream()
                .map(type -> factory.from(type))
                .map(qb -> baseQuery(clusterSegment))
                .collect(toList());
    }

    private String baseQuery(ClusterSegment clusterSegment) {

        String query = this.queryBuilder.buildTermQuery(toProtobufFormat(MetaObject.META_OBJECT_CLUSTER_ID),
                                                        clusterSegment.getClusterId());

        if (clusterSegment.segmentIds().length > 0) {

            String segmentQuery = Arrays.asList(clusterSegment.segmentIds())
                    .stream()
                    .map(segment -> this.queryBuilder.buildTermQuery(toProtobufFormat(MetaObject.META_OBJECT_SEGMENT_ID),
                                                                     segment))
                    .collect(joining(" OR "));

            return new StringBuilder().append(query)
                    .append(" AND ")
                    .append("(")
                    .append(segmentQuery)
                    .append(")")
                    .toString();
        } else {
            return query;
        }
    }

    protected QueryFactory getQueryFactory(String index) {
        return Search
                .getQueryFactory(this.infinispanContext.getCache(index));
    }
}
