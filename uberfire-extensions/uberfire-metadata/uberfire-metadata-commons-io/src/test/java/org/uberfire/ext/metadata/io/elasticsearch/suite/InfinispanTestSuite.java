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

package org.uberfire.ext.metadata.io.elasticsearch.suite;

import org.arquillian.cube.docker.junit.rule.ContainerDslRule;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.uberfire.ext.metadata.io.elasticsearch.BatchIndexConcurrencyTest;
import org.uberfire.ext.metadata.io.elasticsearch.BatchIndexSingleThreadTest;
import org.uberfire.ext.metadata.io.elasticsearch.BatchIndexTest;
import org.uberfire.ext.metadata.io.elasticsearch.ComplexFieldsTest;
import org.uberfire.ext.metadata.io.elasticsearch.ElasticFullTextSearchIndexTest;
import org.uberfire.ext.metadata.io.elasticsearch.ElasticSearchIndexTest;
import org.uberfire.ext.metadata.io.elasticsearch.IOSearchServiceImplTest;
import org.uberfire.ext.metadata.io.elasticsearch.IOServiceIndexedDeleteFileTest;
import org.uberfire.ext.metadata.io.elasticsearch.IOServiceIndexedDotFileGitImplTest;
import org.uberfire.ext.metadata.io.elasticsearch.IOServiceIndexedDotFileGitInternalImplTest;
import org.uberfire.ext.metadata.io.elasticsearch.IOServiceIndexedGitImplTest;
import org.uberfire.ext.metadata.io.elasticsearch.IOServiceIndexedSortingTest;
import org.uberfire.ext.metadata.io.elasticsearch.ReplaceIndexedObjectTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        BatchIndexConcurrencyTest.class,
        BatchIndexSingleThreadTest.class,
        BatchIndexTest.class,
        ComplexFieldsTest.class,
        ElasticFullTextSearchIndexTest.class,
        ElasticSearchIndexTest.class,
        IOSearchServiceImplTest.class,
        IOServiceIndexedDeleteFileTest.class,
        IOServiceIndexedDotFileGitImplTest.class,
        IOServiceIndexedDotFileGitInternalImplTest.class,
        IOServiceIndexedGitImplTest.class,
        IOServiceIndexedSortingTest.class,
        ReplaceIndexedObjectTest.class
})
public class InfinispanTestSuite {

    @ClassRule
    public static ContainerDslRule infinispan = new ContainerDslRule("jboss/infinispan-server:9.2.2.Final")
            .withEnvironment("APP_USER",
                             "user")
            .withEnvironment("APP_PASS",
                             "user")
            .withPortBinding(8080,
                             11222);
}
