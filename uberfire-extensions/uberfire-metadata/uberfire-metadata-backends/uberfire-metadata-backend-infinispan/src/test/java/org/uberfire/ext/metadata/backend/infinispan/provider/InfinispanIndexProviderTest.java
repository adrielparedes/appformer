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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.arquillian.cube.docker.junit.rule.ContainerDslRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.uberfire.ext.metadata.model.KObject;
import org.uberfire.ext.metadata.model.impl.KObjectImpl;

import static org.junit.Assert.*;

public class InfinispanIndexProviderTest {

    @ClassRule
    public static ContainerDslRule infinispan = new ContainerDslRule("jboss/infinispan-server:9.2.2.Final")
            .withEnvironment("APP_USER",
                             "user")
            .withEnvironment("APP_PASS",
                             "user")
            .withPortBinding(8080,
                             11222);

    @Test
    public void test() {
        InfinispanIndexProvider provider = new InfinispanIndexProvider(new InfinispanContext());

        KObject kObject = new KObjectImpl("1",
                                          "String",
                                          "java",
                                          "java",
                                          "key",
                                          Collections.emptyList(),
                                          true);

        provider.index(kObject);

        List<KObject> results = provider.findByQuery(Arrays.asList("java"),
                                                     new TermQuery(new Term("clusterId",
                                                                            "java")),
                                                     10);

        assertTrue(results.size() > 0);
    }
}