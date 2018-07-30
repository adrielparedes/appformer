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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.uberfire.commons.lifecycle.Disposable;
import org.uberfire.ext.metadata.backend.infinispan.proto.KObjectMarshaller;
import org.uberfire.ext.metadata.backend.infinispan.proto.schema.Schema;
import org.uberfire.ext.metadata.backend.infinispan.proto.schema.SchemaGenerator;
import org.uberfire.ext.metadata.backend.infinispan.utils.AttributesUtil;
import org.uberfire.ext.metadata.model.KObject;
import org.uberfire.ext.metadata.model.impl.KObjectImpl;

public class InfinispanContext implements Disposable {

    public static final String TYPES_CACHE = "types";
    public static final String PROTO_EXTENSION = ".proto";
    private final RemoteCacheManager cacheManager;
    private final KieProtostreamMarshaller marshaller = new KieProtostreamMarshaller();
    private final SchemaGenerator schemaGenerator;

    private final Map<String, List<String>> types = new HashMap<>();
    private final InfinispanConfiguration infinispanConfiguration;

    public InfinispanContext() {
        this.infinispanConfiguration = new InfinispanConfiguration();
        schemaGenerator = new SchemaGenerator();
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.addServer()
                .host("127.0.0.1")
                .port(ConfigurationProperties.DEFAULT_HOTROD_PORT)
                .marshaller(marshaller);
        cacheManager = new RemoteCacheManager(builder.build());

        if (!this.getIndices().contains(TYPES_CACHE)) {
            cacheManager.administration().createCache(TYPES_CACHE,
                                                      this.infinispanConfiguration.getConfiguration(TYPES_CACHE));
        }

        marshaller.registerMarshaller(new KieProtostreamMarshaller.KieMarshallerSupplier<KObject>() {
            @Override
            public String extractTypeFromEntity(KObject entity) {
                return AttributesUtil.toProtobufFormat(entity.getType().getName());
            }

            @Override
            public Class<KObject> getJavaClass() {
                return KObject.class;
            }

            @Override
            public BaseMarshaller<KObject> getMarshallerForType(String typeName) {
                return new KObjectMarshaller(typeName);
            }
        });
    }

    public static InfinispanContext getInstance() {
        return new InfinispanContext();
    }

    private RemoteCache<String, String> getProtobufCache() {
        return this.cacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
    }

    public RemoteCache<String, KObject> getCache(String index) {
        if (!this.getIndices().contains(index)) {
            cacheManager
                    .administration()
                    .createCache(index,
                                 this.infinispanConfiguration.getConfiguration(index));
        }
        return this.cacheManager.getCache(index);
    }

    public List<String> getTypes(String index) {
        return this.getTypesCache().getOrDefault(index,
                                                 new ArrayList<>());
    }

    private Map<String, List<String>> getTypesCache() {
//        return this.cacheManager.getCache("types");
        return this.types;
    }

    public List<String> addType(String index,
                                String type) {
        List<String> types = this.getTypes(index);

        String protoType = AttributesUtil.toProtobufFormat(type);

        if (!types.contains(protoType)) {
            types.add(protoType);
        }

        return this.getTypesCache().put(index,
                                        types);
    }

    public void addProtobufSchema(String typeName,
                                  Schema schema) {

        String protoTypeName = AttributesUtil.toProtobufFormat(typeName);
        RemoteCache<String, String> metadataCache = getProtobufCache();
        String proto = this.schemaGenerator.generate(schema);
        try {
            marshaller.registerSchema(protoTypeName,
                                      proto,
                                      KObjectImpl.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        metadataCache.put(protoTypeName + PROTO_EXTENSION,
                          proto);
    }

    @Override
    public void dispose() {
        if (this.cacheManager.isStarted()) {
            this.cacheManager.stop();
        }
    }

    public List<String> getIndices() {
        return this.cacheManager.getCacheNames()
                .stream()
                .collect(Collectors.toList());
    }
}

