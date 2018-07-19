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

package org.uberfire.ext.metadata.backend.infinispan.search;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.uberfire.ext.metadata.search.ClusterSegment;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class InfinispanQueryBuilderTest {

    private InfinispanQueryBuilder queryBuilder;

    @Before
    public void setUp() {
        queryBuilder = new InfinispanQueryBuilder();
    }

    @Test
    public void testBuildSegmentQuery() {
        {
            String[] segments = {};
            Optional<String> result = this.queryBuilder.buildSegmentsQuery(segments);

            assertFalse(result.isPresent());
        }
        {
            String[] segments = {"a.segment"};
            Optional<String> result = this.queryBuilder.buildSegmentsQuery(segments);

            assertEquals("segment__id:'a__segment'",
                         result.get()
            );
        }

        {
            String[] segments = {"a.segment", "a.second.segment"};
            Optional<String> result = this.queryBuilder.buildSegmentsQuery(segments);

            assertEquals("segment__id:'a__segment' OR segment__id:'a__second__segment'",
                         result.get()
            );
        }
    }

    @Test
    public void testBuildCommonQuery() {
        {
            ClusterSegment clusterSegment = mock(ClusterSegment.class);
            when(clusterSegment.getClusterId()).thenReturn("");

            try {
                String result = this.queryBuilder.buildCommonQuery(clusterSegment);
                fail("IllegalArgumentException expected");
            } catch (IllegalArgumentException e) {
            }
        }
        {
            ClusterSegment clusterSegment = mock(ClusterSegment.class);
            when(clusterSegment.getClusterId()).thenReturn("a.cluster.id");

            String result = this.queryBuilder.buildCommonQuery(clusterSegment);

            assertEquals("cluster__id:'a__cluster__id'",
                         result);
        }

        {
            ClusterSegment clusterSegment = mock(ClusterSegment.class);
            when(clusterSegment.getClusterId()).thenReturn("a.cluster.id");
            when(clusterSegment.segmentIds()).thenReturn(new String[]{"a.segment"});

            String result = this.queryBuilder.buildCommonQuery(clusterSegment);

            assertEquals("cluster__id:'a__cluster__id' AND (segment__id:'a__segment')",
                         result);
        }
    }

    @Test
    public void testBuildFullTextTermQuery() {
        {
            try {
                this.queryBuilder.buildFullTextTermQuery("");
                fail("IllegalArgumentException expected");
            } catch (IllegalArgumentException e) {

            }
        }
        {
            String query = this.queryBuilder.buildFullTextTermQuery("aTerm");
            assertEquals("fullText:'*aterm*'",
                         query);
        }
    }

    @Test
    public void testBuildFromQuery() {
        {
            try {
                this.queryBuilder.buildFromQuery("");
                fail("IllegalArgumentException expected");
            } catch (IllegalArgumentException e) {

            }
        }
        {
            String query = this.queryBuilder.buildFromQuery("a.type");
            assertEquals("from a__type",
                         query);
        }
    }

    @Test
    public void testBuildDateRange() {
        String query = this.queryBuilder.buildDateRangeQuery("a.field",
                                                             0,
                                                             1);
        assertEquals("a__field > 0 AND a__field < 1",
                     query);
    }
}