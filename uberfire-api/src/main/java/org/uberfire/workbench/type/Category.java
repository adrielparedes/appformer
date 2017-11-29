/*
 * Copyright 2017 Red Hat, Inc. and/or its affiliates.
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

package org.uberfire.workbench.type;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public enum Category {

    OTHERS,
    DECISION,
    FORM,
    MODEL,
    PROCESS,
    UNDEFINED;

    public static List<Category> getCategories() {
        return Arrays.stream(values()).filter(category -> !category.equals(UNDEFINED)).collect(Collectors.toList());
    }

    public static Category getCategory(String category) {
        try {
            return valueOf(category);
        } catch (Exception e) {
            return Category.UNDEFINED;
        }
    }
}
