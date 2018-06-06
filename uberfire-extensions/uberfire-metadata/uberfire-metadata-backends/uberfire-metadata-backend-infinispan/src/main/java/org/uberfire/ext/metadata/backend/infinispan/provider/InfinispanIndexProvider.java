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

package org.uberfire.ext.metadata.backend.infinispan.provider;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.dsl.QueryFactory;
import org.uberfire.ext.metadata.backend.infinispan.proto.schema.Schema;
import org.uberfire.ext.metadata.model.KCluster;
import org.uberfire.ext.metadata.model.KObject;
import org.uberfire.ext.metadata.provider.IndexProvider;

import static org.kie.soup.commons.validation.PortablePreconditions.checkNotEmpty;
import static org.kie.soup.commons.validation.PortablePreconditions.checkNotNull;

public class InfinispanIndexProvider implements IndexProvider {

    private final InfinispanContext infinispanContext;
    private final QueryFactory queryFactory;

    public InfinispanIndexProvider(InfinispanContext infinispanContext) {
        this.infinispanContext = infinispanContext;
        queryFactory = Search
                .getQueryFactory(this.infinispanContext.getCache());
    }

    @Override
    public boolean isFreshIndex(KCluster cluster) {
        return false;
    }

    @Override
    public void index(KObject kObject) {
        this.infinispanContext.addProtobufSchema(kObject.getClusterId(),
                                                 new Schema(kObject.getClusterId(),
                                                            "org.appformer",
                                                            Collections.emptySet()));
        this.infinispanContext.getCache().put(kObject.getId(),
                                              kObject);
    }

    @Override
    public void index(List<KObject> elements) {

    }

    @Override
    public boolean exists(String index,
                          String id) {
        return false;
    }

    @Override
    public void delete(String index) {

    }

    @Override
    public void delete(String index,
                       String id) {

    }

    @Override
    public List<KObject> findById(String index,
                                  String id) throws IOException {
        return null;
    }

    @Override
    public void rename(String index,
                       String id,
                       KObject to) {

    }

    @Override
    public long getIndexSize(String index) {
        return 0;
    }

    @Override
    public List<KObject> findByQuery(List<String> indices,
                                     Query query,
                                     int limit) {
        return this.findByQuery(indices,
                                query,
                                null,
                                limit);
    }

    @Override
    public List<KObject> findByQuery(List<String> indices,
                                     Query query,
                                     Sort sort,
                                     int limit) {

        List<String> indexes = indices;
        if (indices.isEmpty()) {
            indexes = this.getIndices();
        }

        Optional<String> orderBy = this.buildOrderBy(Optional.ofNullable(sort));

        return indexes.stream()
                .map(index -> this.createInfinispanQuery(index,
                                                         query,
                                                         orderBy).list())
                .flatMap(x -> x.stream())
                .map(x -> (KObject) x)
                .distinct()
                .collect(Collectors.toList());
    }

    private Optional<String> buildOrderBy(Optional<Sort> sort) {
        return Optional.empty();
    }

    @Override
    public long findHitsByQuery(List<String> indices,
                                Query query) {

        List<String> indexes = indices;
        if (indices.isEmpty()) {
            indexes = this.getIndices();
        }

        int totalHits = indexes
                .stream()
                .mapToInt(index -> this.createInfinispanQuery(index,
                                                              query,
                                                              Optional.empty()).getResultSize())
                .sum();

        return totalHits;
    }

    @Override
    public List<String> getIndices() {
        return Collections.emptyList();
    }

    @Override
    public void dispose() {
        this.infinispanContext.dispose();
    }

    private org.infinispan.query.dsl.Query createInfinispanQuery(String index,
                                                                 Query query,
                                                                 Optional<String> orderBy) {

        checkNotEmpty(index,
                      "index");
        checkNotNull("query",
                     query);

        String queryString = this.buildQueryString(index,
                                                   query,
                                                   orderBy);
        return this.getQueryFactory()
                .create(queryString);
    }

    private String buildQueryString(String index,
                                    Query query,
                                    Optional<String> orderBy) {

        String mainQuery = MessageFormat.format("from {0} where {1}",
                                                index,
                                                query);

        return orderBy.map(order -> MessageFormat.format("{0} order by {1}",
                                                         mainQuery,
                                                         order)).orElse(mainQuery);
    }

    private QueryFactory getQueryFactory() {
        return queryFactory;
    }
}
