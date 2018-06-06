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

package org.uberfire.ext.metadata.backend.infinispan.proto.schema;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class SchemaGeneratorTest {

    private Logger logger = LoggerFactory.getLogger(SchemaGeneratorTest.class);

    private SchemaGenerator schemaGenerator;

    @Before
    public void setUp() {
        this.schemaGenerator = new SchemaGenerator();
    }

    @Test
    public void testBuildField() {
        {
            String generatedField = this.schemaGenerator.buildField(new Field(ProtobufScope.REQUIRED,
                                                                              "int32",
                                                                              "aField",
                                                                              1));
            assertEquals("required int32 aField = 1;",
                         generatedField);
        }

        {
            String generatedField = this.schemaGenerator.buildField(new Field(ProtobufScope.OPTIONAL,
                                                                              "int32",
                                                                              "aField",
                                                                              1));
            assertEquals("optional int32 aField = 1;",
                         generatedField);
        }

        {
            String generatedField = this.schemaGenerator.buildField(new Field(ProtobufScope.REPEATED,
                                                                              "int32",
                                                                              "aField",
                                                                              1));
            assertEquals("repeated int32 aField = 1;",
                         generatedField);
        }
    }

    @Test
    public void testBuildMessageWithSingleField() {
        Message message = new Message("KObject",
                                      Collections.emptySet(),
                                      Collections.singleton(new Field(ProtobufScope.REQUIRED,
                                                                      "int32",
                                                                      "aField",
                                                                      1)));
        String generatedMessage = this.schemaGenerator.buildMessage(message);
        logger.debug(generatedMessage);
        assertEquals(this.read("proto/single-field-message.proto"),
                     this.sanitize(generatedMessage));
    }

    @Test
    public void testBuildMessageWithMultipleFields() {
        Message message = new Message("KObject",
                                      Collections.emptySet(),
                                      new HashSet<>(Arrays.asList(new Field(ProtobufScope.REQUIRED,
                                                                            "int32",
                                                                            "aField",
                                                                            1),
                                                                  new Field(ProtobufScope.REQUIRED,
                                                                            "string",
                                                                            "anotherField",
                                                                            2))));
        String generatedMessage = this.schemaGenerator.buildMessage(message);
        logger.debug(generatedMessage);
        assertEquals(this.read("proto/multi-field-message.proto"),
                     this.sanitize(generatedMessage));
    }

    @Test
    public void testBuildMessageWithEmbeddedMessage() {

        Message embeddedMessage = new Message("EmbeddedKObject",
                                              Collections.emptySet(),
                                              Collections.singleton(new Field(ProtobufScope.REQUIRED,
                                                                              "int32",
                                                                              "aField",
                                                                              1)));

        Message message = new Message("KObject",
                                      Collections.singleton(embeddedMessage),
                                      Collections.singleton(new Field(ProtobufScope.REQUIRED,
                                                                      embeddedMessage.getName(),
                                                                      "embedded",
                                                                      1)));
        String generatedMessage = this.schemaGenerator.buildMessage(message);
        logger.debug(generatedMessage);
        assertEquals(this.read("proto/single-embedded-message.proto"),
                     this.sanitize(generatedMessage));
    }

    @Test
    public void testBuildMessageWithMultipleEmbeddedMessage() {

        Message embeddedMessage = new Message("EmbeddedKObject",
                                              Collections.emptySet(),
                                              Collections.singleton(new Field(ProtobufScope.REQUIRED,
                                                                              "int32",
                                                                              "aField",
                                                                              1)));

        Message anotherEmbeddedMessage = new Message("AnotherEmbeddedKObject",
                                                     Collections.emptySet(),
                                                     Collections.singleton(new Field(ProtobufScope.REQUIRED,
                                                                                     "int32",
                                                                                     "aField",
                                                                                     1)));

        Message message = new Message("KObject",
                                      new HashSet<>(Arrays.asList(embeddedMessage,
                                                                  anotherEmbeddedMessage)),
                                      new HashSet<>(Arrays.asList(new Field(ProtobufScope.REQUIRED,
                                                                            embeddedMessage.getName(),
                                                                            "embedded",
                                                                            1),
                                                                  new Field(ProtobufScope.REQUIRED,
                                                                            embeddedMessage.getName(),
                                                                            "anotherEmbedded",
                                                                            2))
                                      ));
        String generatedMessage = this.schemaGenerator.buildMessage(message);
        logger.debug(generatedMessage);
        assertEquals(this.read("proto/multi-embedded-message.proto"),
                     this.sanitize(generatedMessage));
    }

    @Test
    public void testBuildSchema() {

        Message embeddedMessage = new Message("EmbeddedKObject",
                                              Collections.emptySet(),
                                              Collections.singleton(new Field(ProtobufScope.REQUIRED,
                                                                              "int32",
                                                                              "aField",
                                                                              1)));

        Message anotherEmbeddedMessage = new Message("AnotherEmbeddedKObject",
                                                     Collections.emptySet(),
                                                     Collections.singleton(new Field(ProtobufScope.REQUIRED,
                                                                                     "int32",
                                                                                     "aField",
                                                                                     1)));

        Message message = new Message("KObject",
                                      new HashSet<>(Arrays.asList(embeddedMessage,
                                                                  anotherEmbeddedMessage)),
                                      new HashSet<>(Arrays.asList(new Field(ProtobufScope.REQUIRED,
                                                                            embeddedMessage.getName(),
                                                                            "embedded",
                                                                            1),
                                                                  new Field(ProtobufScope.REQUIRED,
                                                                            embeddedMessage.getName(),
                                                                            "anotherEmbedded",
                                                                            2))
                                      ));

        Schema schema = new Schema("KObjectSchema",
                                   "org.appformer",
                                   Collections.singleton(message));

        String generatedMessage = this.schemaGenerator.generate(schema);
        logger.debug(generatedMessage);
        assertEquals(this.read("proto/schema.proto"),
                     this.sanitize(generatedMessage));
    }

    private String read(String file) {
        URL url = Resources.getResource(file);
        try {
            return sanitize(Resources.toString(url,
                                               Charsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String sanitize(String value) {
        return value.replaceAll("\t",
                                "").replace("\n",
                                            "");
    }
}