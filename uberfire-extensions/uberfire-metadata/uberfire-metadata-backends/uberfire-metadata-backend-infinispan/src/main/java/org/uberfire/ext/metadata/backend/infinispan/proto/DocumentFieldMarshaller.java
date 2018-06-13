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

import org.infinispan.protostream.MessageMarshaller;
import org.uberfire.java.nio.base.version.VersionHistory;
import org.uberfire.java.nio.file.attribute.FileTime;

public class DocumentFieldMarshaller implements MessageMarshaller<DocumentField> {

    public static final String CHECKIN_COMMENT = "checkinComment";
    public static final String LAST_MODIFIED_BY = "lastModifiedBy";
    public static final String CREATED_BY = "createdBy";
    public static final String CREATED_DATE = "createdDate";
    public static final String LAST_MODIFIED_DATE = "lastModifiedDate";

    @Override
    public DocumentField readFrom(ProtoStreamReader reader) throws IOException {


        return null;
    }

    @Override
    public void writeTo(ProtoStreamWriter writer,
                        DocumentField documentField) throws IOException {

        String name = documentField.getName();
        Object value = documentField.getValue();

        this.build(name,
                   value,
                   writer);
    }

    private void build(String name,
                       Object value,
                       ProtoStreamWriter writer) throws IOException {
        if (value instanceof DocumentField) {
            writer.writeObject(name,
                               value,
                               DocumentField.class);
        } else {

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

    @Override
    public Class<? extends DocumentField> getJavaClass() {
        return DocumentField.class;
    }

    @Override
    public String getTypeName() {
        return "DocumentField";
    }
}
