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

package org.uberfire.ext.metadata.backend.infinispan.proto;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.infinispan.protostream.MessageMarshaller;
import org.uberfire.ext.metadata.model.KObject;
import org.uberfire.ext.metadata.model.KProperty;
import org.uberfire.ext.metadata.model.impl.KObjectImpl;

public class KObjectMarshaller implements MessageMarshaller<KObjectImpl> {

    public static final String SEGMENT_ID = "segmentId";
    public static final String CLUSTER_ID = "clusterId";
    public static final String TYPE = "type";
    public static final String ID = "id";
    public static final String KEY = "key";
    public static final String FULL_TEXT = "fullText";
    private String typeName;

    public KObjectMarshaller(String typeName) {

        this.typeName = typeName;
    }

    public KObjectMarshaller() {

        this.typeName = "java";
    }

    @Override
    public KObjectImpl readFrom(ProtoStreamReader protoStreamReader) throws IOException {
        String id = protoStreamReader.readString(ID);
        String type = protoStreamReader.readString(TYPE);
        String clusterId = protoStreamReader.readString(CLUSTER_ID);
        String segmentId = protoStreamReader.readString(SEGMENT_ID);
        String key = protoStreamReader.readString(KEY);
        List<KProperty<?>> properties = Collections.emptyList();
        boolean fullText = protoStreamReader.readBoolean(FULL_TEXT);

        return new KObjectImpl(id,
                               type,
                               clusterId,
                               segmentId,
                               key,
                               properties,
                               fullText);
    }

    @Override
    public void writeTo(ProtoStreamWriter protoStreamWriter,
                        KObjectImpl kObject) throws IOException {
        protoStreamWriter.writeString(ID,
                                      kObject.getId());
        protoStreamWriter.writeString(TYPE,
                                      kObject.getType().getName());
        protoStreamWriter.writeString(CLUSTER_ID,
                                      kObject.getClusterId());
        protoStreamWriter.writeString(SEGMENT_ID,
                                      kObject.getClusterId());
        protoStreamWriter.writeString(KEY,
                                      kObject.getKey());
        protoStreamWriter.writeBoolean(FULL_TEXT,
                                       kObject.fullText());
    }

    @Override
    public Class<? extends KObjectImpl> getJavaClass() {
        return KObjectImpl.class;
    }

    @Override
    public String getTypeName() {
        return this.typeName;
    }
}
