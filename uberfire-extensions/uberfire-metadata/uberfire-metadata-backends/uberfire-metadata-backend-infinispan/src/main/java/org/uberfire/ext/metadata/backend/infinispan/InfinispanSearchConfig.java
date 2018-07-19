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

package org.uberfire.ext.metadata.backend.infinispan;

import org.uberfire.ext.metadata.MetadataConfig;
import org.uberfire.ext.metadata.backend.infinispan.provider.InfinispanContext;
import org.uberfire.ext.metadata.backend.infinispan.provider.InfinispanIndexProvider;
import org.uberfire.ext.metadata.backend.infinispan.provider.MappingProvider;
import org.uberfire.ext.metadata.backend.infinispan.search.InfinispanSearchIndex;
import org.uberfire.ext.metadata.engine.MetaIndexEngine;
import org.uberfire.ext.metadata.engine.MetaModelStore;
import org.uberfire.ext.metadata.metamodel.NullMetaModelStore;
import org.uberfire.ext.metadata.provider.IndexProvider;
import org.uberfire.ext.metadata.search.SearchIndex;

public class InfinispanSearchConfig implements MetadataConfig {

    private final InfinispanIndexProvider indexProvider;
    private final InfinispanContext infinispanContext;
    private final MappingProvider mappingProvider;
    private final InfinispanSearchIndex searchIndex;
    private MetaIndexEngine metaIndexEngine;

    public InfinispanSearchConfig(MetaIndexEngine metaIndexEngine) {
        this.metaIndexEngine = metaIndexEngine;
        this.infinispanContext = new InfinispanContext();
        this.mappingProvider = new MappingProvider();
        this.indexProvider = new InfinispanIndexProvider(this.infinispanContext,
                                                         this.mappingProvider);
        this.searchIndex = new InfinispanSearchIndex(this.infinispanContext);
    }

    @Override
    public IndexProvider getIndexProvider() {
        return this.indexProvider;
    }

    @Override
    public SearchIndex getSearchIndex() {
        return this.searchIndex;
    }

    @Override
    public MetaIndexEngine getIndexEngine() {
        return this.metaIndexEngine;
    }

    @Override
    public MetaModelStore getMetaModelStore() {
        return new NullMetaModelStore();
    }

    @Override
    public int priority() {
        return -20;
    }

    @Override
    public void dispose() {
        this.indexProvider.dispose();
        this.metaIndexEngine.dispose();
    }
}
