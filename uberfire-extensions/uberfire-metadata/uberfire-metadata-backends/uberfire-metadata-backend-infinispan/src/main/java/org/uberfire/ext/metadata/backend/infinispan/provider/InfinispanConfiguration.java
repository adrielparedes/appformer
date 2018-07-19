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

import java.io.InputStream;
import java.util.Scanner;

import org.infinispan.commons.configuration.XMLStringConfiguration;

public class InfinispanConfiguration {

    public static final String INFINISPAN_TEMPLATE = "infinispan-template.xml";
    
    private final String template;

    public InfinispanConfiguration() {
        this.template = this.loadTemplate();
    }

    private String loadTemplate() {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(INFINISPAN_TEMPLATE);
        Scanner s = new Scanner(inputStream).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }

    public XMLStringConfiguration getConfiguration(String value) {
        return new XMLStringConfiguration(this.template.replaceAll("%value%",
                                                                   value));
    }
}
