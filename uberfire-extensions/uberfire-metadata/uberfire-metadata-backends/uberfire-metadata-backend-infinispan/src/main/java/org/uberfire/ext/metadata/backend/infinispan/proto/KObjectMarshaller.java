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
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.descriptors.Descriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uberfire.ext.metadata.model.KProperty;
import org.uberfire.ext.metadata.model.impl.KObjectImpl;
import org.uberfire.ext.metadata.model.impl.KPropertyImpl;
import org.uberfire.ext.metadata.model.schema.MetaObject;
import org.uberfire.java.nio.base.version.VersionHistory;
import org.uberfire.java.nio.file.attribute.FileTime;

import static java.util.stream.Collectors.toList;

public class KObjectMarshaller implements MessageMarshaller<KObjectImpl> {

    private Logger logger = LoggerFactory.getLogger(KObjectMarshaller.class);

    public static final String CHECKIN_COMMENT = "checkinComment";
    public static final String LAST_MODIFIED_BY = "lastModifiedBy";
    public static final String CREATED_BY = "createdBy";
    public static final String CREATED_DATE = "createdDate";
    public static final String LAST_MODIFIED_DATE = "lastModifiedDate";

    public static final String SEGMENT_ID = "segment__id";
    public static final String CLUSTER_ID = "cluster__id";
    public static final String TYPE = "type";
    public static final String ID = "id";
    public static final String KEY = "key";
    public static final String FULL_TEXT = "fullText";
    private String typeName;

    public KObjectMarshaller(String typeName) {

        this.typeName = typeName;
    }

    @Override
    public KObjectImpl readFrom(ProtoStreamReader protoStreamReader) throws IOException {

        Descriptor descriptor = protoStreamReader.getSerializationContext().getMessageDescriptor(this.getTypeName());

        List<KProperty<?>> properties = descriptor.getFields()
                .stream()
                .filter(fieldDescriptor ->
                                isExtension(descriptor.getName())
                )
                .map(field ->
                             (KProperty<?>) new KPropertyImpl(this.toKPropertyFormat(field.getName()),
                                                              field.getDefaultValue(),
                                                              false)
                )
                .collect(toList());

        String id = protoStreamReader.readString(ID);
        String type = protoStreamReader.readString(TYPE);
        String clusterId = protoStreamReader.readString(CLUSTER_ID);
        String segmentId = protoStreamReader.readString(SEGMENT_ID);
        String key = protoStreamReader.readString(KEY);
        String fullText = protoStreamReader.readString(FULL_TEXT);

        return new KObjectImpl(id,
                               type,
                               clusterId,
                               segmentId,
                               key,
                               properties,
                               !fullText.isEmpty());
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
                                      kObject.getSegmentId());
        protoStreamWriter.writeString(KEY,
                                      kObject.getKey());

        kObject.getProperties()
                .iterator()
                .forEachRemaining(kProperty -> {
                    try {
                        this.build(this.toProtobufFormat(kProperty.getName()),
                                   kProperty.getValue(),
                                   protoStreamWriter);
                    } catch (IOException e) {
                        logger.error("error",
                                     e);
                    }
                });

        if (kObject.fullText()) {
            protoStreamWriter.writeString(FULL_TEXT,
                                          "");
        }
    }

    @Override
    public Class<? extends KObjectImpl> getJavaClass() {
        return KObjectImpl.class;
    }

    @Override
    public String getTypeName() {
        return this.typeName;
    }

    private void build(String name,
                       Object value,
                       ProtoStreamWriter writer) throws IOException {

        Class<?> aClass = value.getClass();

        if (Enum.class.isAssignableFrom(aClass)) {
            writer.writeString(name,
                               value.toString());
        }
        if (aClass == String.class) {
            writer.writeString(name,
                               value.toString());
        }
        if (aClass == Boolean.class) {
            writer.writeBoolean(name,
                                (Boolean) value);
        }

        if (aClass == Integer.class) {
            writer.writeInt(name,
                            (Integer) value);
        }

        if (aClass == Double.class) {
            writer.writeDouble(name,
                               (Double) value);
        }

        if (aClass == Long.class) {
            writer.writeLong(name,
                             (Long) value);
        }

        if (aClass == Float.class) {
            writer.writeFloat(name,
                              (Float) value);
        }

        if (FileTime.class.isAssignableFrom(aClass)) {
            writer.writeLong(name,
                             ((FileTime) value).toMillis());
        }

        if (Date.class.isAssignableFrom(aClass)) {
            writer.writeLong(name,
                             ((Date) value).getTime());
        }

        if (VersionHistory.class.isAssignableFrom(aClass)) {
            this.build((VersionHistory) value,
                       writer);
        }

        if (Collection.class.isAssignableFrom(aClass)) {
            final StringBuilder sb = new StringBuilder();
            for (final java.lang.Object oValue : (Collection) value) {
                sb.append(oValue).append(' ');
            }

            writer.writeString(name,
                               sb.toString());
        }
    }

    private void build(VersionHistory versionHistory,
                       ProtoStreamWriter writer) throws IOException {

        if (versionHistory.records().size() != 0) {

            final int lastIndex = versionHistory.records().size() - 1;

            this.build(CHECKIN_COMMENT,
                       versionHistory.records().get(lastIndex).comment(),
                       writer);

            this.build(CREATED_BY,
                       versionHistory.records().get(0).author(),
                       writer);

            this.build(CREATED_DATE,
                       versionHistory.records().get(0).date(),
                       writer);

            this.build(LAST_MODIFIED_BY,
                       versionHistory.records().get(lastIndex).author(),
                       writer);

            this.build(LAST_MODIFIED_DATE,
                       versionHistory.records().get(lastIndex).date(),
                       writer);
        }
    }

    private boolean isExtension(final String name) {
        return !(name.equals(MetaObject.META_OBJECT_ID) ||
                name.equals(MetaObject.META_OBJECT_TYPE) ||
                name.equals(this.toProtobufFormat(MetaObject.META_OBJECT_CLUSTER_ID)) ||
                name.equals(this.toProtobufFormat(MetaObject.META_OBJECT_SEGMENT_ID)) ||
                name.equals(MetaObject.META_OBJECT_KEY));
    }

    private String toProtobufFormat(String value) {
        return value.replaceAll("\\.",
                                "__");
    }

    private String toKPropertyFormat(String value) {
        return value.replaceAll("__",
                                "\\.");
    }
}
