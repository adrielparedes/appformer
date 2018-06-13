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

import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uberfire.ext.metadata.model.impl.KObjectImpl;

public class KObjectMarshallerProvider implements SerializationContext.MarshallerProvider {

    private Logger logger = LoggerFactory.getLogger(KObjectMarshallerProvider.class);

    @Override
    public BaseMarshaller<?> getMarshaller(String typeName) {

        if (typeName.equals("Cluster")) {
            return new DocumentFieldMarshaller();
        } else {
            return new KObjectMarshaller(typeName);
        }
    }

    @Override
    public BaseMarshaller<?> getMarshaller(Class<?> javaClass) {

        if (javaClass.equals(KObjectImpl.class)) {
            return new KObjectMarshaller("org.appformer.String");
        } else if (javaClass.equals(DocumentField.class)) {
            return new DocumentFieldMarshaller();
        } else {
            return null;
        }
    }
}
