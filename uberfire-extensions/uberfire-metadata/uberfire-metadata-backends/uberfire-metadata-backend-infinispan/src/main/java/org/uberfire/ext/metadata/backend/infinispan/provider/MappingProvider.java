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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.uberfire.ext.metadata.backend.infinispan.proto.schema.Field;
import org.uberfire.ext.metadata.backend.infinispan.proto.schema.Message;
import org.uberfire.ext.metadata.backend.infinispan.proto.schema.ProtobufScope;
import org.uberfire.ext.metadata.backend.infinispan.proto.schema.ProtobufType;
import org.uberfire.ext.metadata.backend.infinispan.proto.schema.Schema;
import org.uberfire.ext.metadata.model.KObject;
import org.uberfire.ext.metadata.model.KProperty;

public class MappingProvider {

    public Schema getMapping(KObject kObject) {
        return this.buildProtobuf(kObject);
    }

    private Schema buildProtobuf(KObject kObject) {
        return new Schema(kObject.getClusterId(),
                          "org.appformer",
                          this.buildMessages(kObject));
    }

    private Set<Message> buildMessages(KObject kObject) {

        Set<Field> fields = new HashSet<>();
        fields.add(new Field(ProtobufScope.REQUIRED,
                             ProtobufType.STRING,
                             "id",
                             1));
        fields.add(new Field(ProtobufScope.REQUIRED,
                             ProtobufType.STRING,
                             "type",
                             2));
        fields.add(new Field(ProtobufScope.REQUIRED,
                             ProtobufType.STRING,
                             "cluster__id",
                             3,
                             true,
                             true));
        fields.add(new Field(ProtobufScope.REQUIRED,
                             ProtobufType.STRING,
                             "segment__id",
                             4));
        fields.add(new Field(ProtobufScope.REQUIRED,
                             ProtobufType.STRING,
                             "key",
                             5));
        fields.add(new Field(ProtobufScope.REQUIRED,
                             ProtobufType.STRING,
                             "fullText",
                             6));

        int index = 7;
        Iterator<KProperty<?>> iterator = kObject.getProperties().iterator();
        if (iterator.hasNext()) {
            KProperty<?> prop = iterator.next();
            fields.add(new Field(ProtobufScope.REQUIRED,
                                 this.buildType(prop.getValue()),
                                 this.sanitize(prop.getName()),
                                 index,
                                 prop.isSortable(),
                                 prop.isSearchable()));
            index++;
        }

        Set<Message> messages = new HashSet<>();
        Message message = new Message(kObject.getType().getName(),
                                      messages,
                                      fields);

        return Collections.singleton(message);
    }

    private String sanitize(String name) {
        return name.replaceAll("\\.",
                               "__");
    }

    private ProtobufType buildType(Object value) {
        if (value.getClass() == Integer.class) {
            return ProtobufType.INT32;
        } else {
            return ProtobufType.STRING;
        }
    }
}
