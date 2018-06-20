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

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.uberfire.commons.lifecycle.Disposable;
import org.uberfire.ext.metadata.backend.infinispan.proto.KObjectMarshaller;
import org.uberfire.ext.metadata.backend.infinispan.proto.schema.Schema;
import org.uberfire.ext.metadata.backend.infinispan.proto.schema.SchemaGenerator;
import org.uberfire.ext.metadata.model.KObject;
import org.uberfire.ext.metadata.model.impl.KObjectImpl;

import java.io.IOException;

public class InfinispanContext implements Disposable {

    private final RemoteCacheManager cacheManager;
    private final KieProtostreamMarshaller marshaller = new KieProtostreamMarshaller();
    private final SchemaGenerator schemaGenerator;

    public InfinispanContext() {
        schemaGenerator = new SchemaGenerator();
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.addServer()
                .host("127.0.0.1")
                .port(ConfigurationProperties.DEFAULT_HOTROD_PORT)
                .marshaller(marshaller);
        cacheManager = new RemoteCacheManager(builder.build());

        cacheManager.administration().createCache("cache",
                                                  new XMLStringConfiguration("<infinispan>\n" +
                                                                                     "  <cache-container>\n" +
                                                                                     "    <replicated-cache name=\"cache\">\n" +
                                                                                     "      <indexing index=\"LOCAL\" auto-config=\"true\"/>\n" +
                                                                                     "    </replicated-cache>\n" +
                                                                                     "  </cache-container>\n" +
                                                                                     "</infinispan>"));

        marshaller.registerMarshaller(new KieProtostreamMarshaller.KieMarshallerSupplier<KObjectImpl>() {
            @Override
            public String extractTypeFromEntity(KObjectImpl entity) {
                return entity.getType().getName();
            }

            @Override
            public Class<KObjectImpl> getJavaClass() {
                return KObjectImpl.class;
            }

            @Override
            public BaseMarshaller<KObjectImpl> getMarshallerForType(String typeName) {
                return new KObjectMarshaller(typeName);
            }
        });
    }

    private RemoteCache<String, String> getProtobufCache() {
        return this.cacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
    }

    public RemoteCache<String, KObject> getCache() {
        return this.cacheManager.getCache("cache");
    }

    public void addProtobufSchema(String typeName,
                                  Schema schema) {

        RemoteCache<String, String> metadataCache = getProtobufCache();
        String proto = this.schemaGenerator.generate(schema);
        try {
            marshaller.registerSchema(typeName, proto, KObjectImpl.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        metadataCache.put(typeName + ".proto",
                          proto);
    }

    @Override
    public void dispose() {
        this.cacheManager.stop();
    }
}

