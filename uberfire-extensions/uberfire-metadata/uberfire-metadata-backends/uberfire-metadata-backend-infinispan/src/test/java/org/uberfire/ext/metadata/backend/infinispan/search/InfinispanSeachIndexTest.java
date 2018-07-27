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

import java.text.MessageFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.uberfire.ext.metadata.backend.infinispan.provider.InfinispanContext;
import org.uberfire.ext.metadata.search.ClusterSegment;
import org.uberfire.ext.metadata.search.DateRange;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class InfinispanSeachIndexTest {

    @Mock
    private InfinispanContext infinispanContext;

    private InfinispanSearchIndex infinispanSearchIndex;

    @Before
    public void setUp() {
        infinispanSearchIndex = new InfinispanSearchIndex(infinispanContext);
    }

    @Test
    public void testAttributesQuery() {

        {
            Map<String, Object> attrs = new HashMap<>();
            attrs.put("an.Attribute",
                      "StringContent");

            String result = this.infinispanSearchIndex.attributesQuery(attrs);

            assertEquals("an__Attribute:'*stringcontent*'",
                         result);
        }

        {

            Date date = new Date();

            Map<String, Object> attrs = new HashMap<>();
            attrs.put("an.Attribute",
                      "StringContent");

            attrs.put("another.Attribute",
                      buildDateRange(date));

            String result = this.infinispanSearchIndex.attributesQuery(attrs);

            assertEquals(MessageFormat.format("another__Attribute > {0} AND another__Attribute < {0} AND an__Attribute:''*stringcontent*''",
                                              Long.toString(date.getTime())),
                         result);
        }
    }

    @Test
    public void testBaseQuery() {
        {
            ClusterSegment clusterSegment = buildClusterSegment("a.Cluster.Id");
            String result = this.infinispanSearchIndex.baseQuery(clusterSegment);

            assertEquals("cluster__id:'a.cluster.id'",
                         result);
        }

        {
            ClusterSegment clusterSegment = buildClusterSegment("a.Cluster.Id",
                                                                "the.First.Segment");
            String result = this.infinispanSearchIndex.baseQuery(clusterSegment);

            assertEquals("cluster__id:'a.cluster.id' AND (segment__id:'the.first.segment')",
                         result);
        }

        {
            ClusterSegment clusterSegment = buildClusterSegment("a.Cluster.Id",
                                                                "the.First.Segment",
                                                                "the.Second.Segment");
            String result = this.infinispanSearchIndex.baseQuery(clusterSegment);

            assertEquals("cluster__id:'a.cluster.id' AND (segment__id:'the.first.segment' OR segment__id:'the.second.segment')",
                         result);
        }
    }

    @Test
    public void testInitializeQuery() {

        when(this.infinispanContext.getTypes(any())).thenReturn(Collections.singletonList("a.Type"));

        ClusterSegment clusterSegment = buildClusterSegment("a.Cluster.Id");
        List<InfinispanSearchIndex.QueryContainer> result = this.infinispanSearchIndex.initializeQuery(clusterSegment);

        assertEquals("from org.kie.a__Type where cluster__id:'a.cluster.id'",
                     result.get(0).getQuery());
    }

    @Test
    public void testBuildSearchByAttrsQuery() {
        when(this.infinispanContext.getTypes(any())).thenReturn(Collections.singletonList("a.Type"));

        {
            Map<String, Object> attrs = new HashMap<>();
            attrs.put("an.Attribute",
                      "StringContent");

            List<InfinispanSearchIndex.QueryContainer> result = this.infinispanSearchIndex.buildSearchByAttrsQuery(attrs,
                                                                                                                   new ClusterSegment[]{this.buildClusterSegment("a.Cluster.Id")});

            assertEquals("from org.kie.a__Type where cluster__id:'a.cluster.id' AND an__Attribute:'*stringcontent*'",
                         result.get(0).getQuery());
        }

        {
            Map<String, Object> attrs = new HashMap<>();
            attrs.put("an.Attribute",
                      "StringContent");

            List<InfinispanSearchIndex.QueryContainer> result = this.infinispanSearchIndex.buildSearchByAttrsQuery(attrs,
                                                                                                                   new ClusterSegment[]{this.buildClusterSegment("a.Cluster.Id",
                                                                                                                                                                 "a.Segment")});

            assertEquals("from org.kie.a__Type where cluster__id:'a.cluster.id' AND (segment__id:'a.segment') AND an__Attribute:'*stringcontent*'",
                         result.get(0).getQuery());
        }
    }

    private DateRange buildDateRange(Date date) {
        return new DateRange() {
            @Override
            public Date before() {
                return date;
            }

            @Override
            public Date after() {
                return date;
            }
        };
    }

    private ClusterSegment buildClusterSegment(String clusterId,
                                               String... segments) {
        return new ClusterSegment() {
            @Override
            public String getClusterId() {
                return clusterId;
            }

            @Override
            public String[] segmentIds() {
                return segments;
            }
        };
    }
}
