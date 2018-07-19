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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.dsl.QueryFactory;
import org.uberfire.ext.metadata.backend.infinispan.proto.schema.Schema;
import org.uberfire.ext.metadata.model.KCluster;
import org.uberfire.ext.metadata.model.KObject;
import org.uberfire.ext.metadata.model.schema.MetaObject;
import org.uberfire.ext.metadata.provider.IndexProvider;

import static java.util.stream.Collectors.toList;
import static org.kie.soup.commons.validation.PortablePreconditions.checkCondition;
import static org.kie.soup.commons.validation.PortablePreconditions.checkNotEmpty;
import static org.kie.soup.commons.validation.PortablePreconditions.checkNotNull;

public class InfinispanIndexProvider implements IndexProvider {

    private final InfinispanContext infinispanContext;
    private final MappingProvider mappingProvider;

    public InfinispanIndexProvider(InfinispanContext infinispanContext,
                                   MappingProvider mappingProvider) {
        this.infinispanContext = infinispanContext;
        this.mappingProvider = mappingProvider;
    }

    @Override
    public boolean isFreshIndex(KCluster cluster) {
        return false;
    }

    @Override
    public void index(KObject kObject) {

        Schema schema = this.mappingProvider.getMapping(kObject);

        this.infinispanContext.addProtobufSchema(kObject.getType().getName(),
                                                 schema);

        this.infinispanContext.addType(kObject.getClusterId(),
                                       kObject.getType().getName());

        this.infinispanContext.getCache(kObject.getClusterId()).put(kObject.getId(),
                                                                    kObject);
    }

    @Override
    public void index(List<KObject> elements) {
        Map<String, KObject> bulk = elements.stream().collect(Collectors.toMap(KObject::getId,
                                                                               kObject -> kObject));

        this.infinispanContext.getCache("").putAll(bulk);
    }

    @Override
    public boolean exists(String index,
                          String id) {
        return this.infinispanContext.getCache(index).containsKey(id);
    }

    @Override
    public void delete(String index) {

    }

    @Override
    public void delete(String index,
                       String id) {
        KObject kObject = this.infinispanContext.getCache(index).get(id);
        this.infinispanContext.getCache(index).remove(kObject);
    }

    @Override
    public List<KObject> findById(String index,
                                  String id) throws IOException {

        List<String> types = this.infinispanContext.getTypes(index);

        return types
                .stream()
                .map(type -> this.getQueryFactory(index)
                        .from(type)
                        .having(MetaObject.META_OBJECT_ID)
                        .eq(id)
                        .build()
                        .list())
                .flatMap(x -> x.stream())
                .map(x -> (KObject) x)
                .collect(toList());
    }

    @Override
    public void rename(String index,
                       String id,
                       KObject to) {

        checkNotEmpty("from",
                      index);
        checkNotEmpty("id",
                      id);
        checkNotNull("to",
                     to);
        checkNotEmpty("clusterId",
                      to.getClusterId());

        checkCondition("renames are allowed only from same cluster",
                       to.getClusterId().equals(index));

        if (this.exists(index,
                        id)) {
            this.delete(index,
                        id);
            this.index(to);
        }
    }

    @Override
    public long getIndexSize(String index) {
        return this.infinispanContext.getCache(index).size();
    }

    @Override
    public List<KObject> findByQuery(List<String> indices,
                                     Query query,
                                     int limit) {

        throw new UnsupportedOperationException();
    }

    @Override
    public List<KObject> findByQuery(List<String> indices,
                                     Query query,
                                     Sort sort,
                                     int limit) {

        throw new UnsupportedOperationException();
    }

    @Override
    public long findHitsByQuery(List<String> indices,
                                Query query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getIndices() {
        return this.infinispanContext.getIndices();
    }

    protected QueryFactory getQueryFactory(String index) {
        return Search
                .getQueryFactory(this.infinispanContext.getCache(index));
    }

    @Override
    public void dispose() {
        this.infinispanContext.dispose();
    }
}
