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

import java.util.Arrays;
import java.util.Optional;

import org.uberfire.ext.metadata.model.schema.MetaObject;
import org.uberfire.ext.metadata.search.ClusterSegment;

import static java.util.stream.Collectors.joining;
import static org.kie.soup.commons.validation.PortablePreconditions.checkNotEmpty;
import static org.kie.soup.commons.validation.PortablePreconditions.checkNotNull;
import static org.uberfire.ext.metadata.backend.infinispan.utils.AttributesUtil.toProtobufFormat;

public class InfinispanQueryBuilder {

    public String buildFullTextTermQuery(String term) {

        return this.buildWildcardQuery(MetaObject.META_OBJECT_FULL_TEXT,
                                       term);
    }

    public String buildTermQuery(String field,
                                 String term) {
        checkNotEmpty("term",
                      term);

        return new StringBuilder()
                .append(toProtobufFormat(field))
                .append(":")
                .append("'")
                .append(term.toLowerCase())
                .append("'")
                .toString();
    }

    public String buildWildcardQuery(String field,
                                     String term) {

        return this.buildTermQuery(field,
                                   "*" + term + "*");
    }

    public String buildFromQuery(String type) {

        checkNotEmpty("type",
                      type);

        return new StringBuilder()
                .append("from")
                .append(" ")
                .append("org.kie.")
                .append(toProtobufFormat(type))
                .toString();
    }

    public String buildCommonQuery(ClusterSegment clusterSegment) {

        checkNotNull("clusterSegment",
                     clusterSegment);
        checkNotEmpty("cluster.id",
                      clusterSegment.getClusterId());

        StringBuilder builder = new StringBuilder()
                .append(toProtobufFormat(MetaObject.META_OBJECT_CLUSTER_ID))
                .append(":")
                .append("'")
                .append(toProtobufFormat(clusterSegment.getClusterId()))
                .append("'");

        Optional<String> segmentsQuery = buildSegmentsQuery(clusterSegment.segmentIds());

        segmentsQuery.ifPresent(q -> builder
                .append(" ")
                .append("AND")
                .append(" ")
                .append("(")
                .append(q)
                .append(")"));

        return builder.toString();
    }

    public Optional<String> buildSegmentsQuery(String[] segments) {

        if (segments == null) {
            return Optional.empty();
        }

        String query = Arrays.asList(segments)
                .stream()
                .map(segment -> new StringBuilder()
                        .append(toProtobufFormat(MetaObject.META_OBJECT_SEGMENT_ID))
                        .append(":")
                        .append("'")
                        .append(toProtobufFormat(segment))
                        .append("'")
                        .toString())
                .collect(joining(" OR "));

        if (query.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(query);
        }
    }

    public String buildDateRangeQuery(String field,
                                      long from,
                                      long to) {

        checkNotEmpty("field",
                      field);

        String f = toProtobufFormat(field);

        return new StringBuilder()
                .append(f)
                .append(" > ")
                .append(from)
                .append(" AND ")
                .append(f)
                .append(" < ")
                .append(to)
                .toString();
    }

    public String buildBooleanQuery(String field,
                                    boolean value) {

        checkNotEmpty("field",
                      field);

        String f = toProtobufFormat(field);

        return new StringBuilder()
                .append(f)
                .append(":")
                .append(value ? "0" : "1")
                .toString();
    }
}
