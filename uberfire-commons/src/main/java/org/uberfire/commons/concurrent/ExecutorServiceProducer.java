/*
 * Copyright 2017 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.uberfire.commons.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.uberfire.commons.async.DescriptiveThreadFactory;

/**
 * ExecutorService Producer. It produces managed and unmanaged executor services. For now the implementation is the same
 * but it could change if any other container gets under support. They are in different variables on purpose.
 */
public class ExecutorServiceProducer {

    private final ExecutorService executorService;
    private final ExecutorService unmanagedExecutorService;

    protected static final String MANAGED_LIMIT_PROPERTY = "org.appformer.concurrent.managed.thread.limit";
    protected static final String UNMANAGED_LIMIT_PROPERTY = "org.appformer.concurrent.unmanaged.thread.limit";

    public ExecutorServiceProducer() {
        this.executorService = this.buildFixedThreadPoolExecutorService(MANAGED_LIMIT_PROPERTY);
        this.unmanagedExecutorService = this.buildFixedThreadPoolExecutorService(UNMANAGED_LIMIT_PROPERTY);
    }

    protected ExecutorService buildFixedThreadPoolExecutorService(String key) {
        String stringProperty = System.getProperty(key);
        int threadLimit = stringProperty == null ? 0 : Integer.valueOf(stringProperty);
        if (threadLimit > 0) {
            return Executors.newFixedThreadPool(threadLimit,
                                                new DescriptiveThreadFactory());
        } else {
            return Executors.newCachedThreadPool(new DescriptiveThreadFactory());
        }
    }

    @Produces
    @ApplicationScoped
    @Managed
    public ExecutorService produceExecutorService() {
        return this.getManagedExecutorService();
    }

    @Produces
    @ApplicationScoped
    @Unmanaged
    public ExecutorService produceUnmanagedExecutorService() {
        return this.getUnmanagedExecutorService();
    }

    protected ExecutorService getManagedExecutorService() {
        return this.executorService;
    }

    protected ExecutorService getUnmanagedExecutorService() {
        return this.unmanagedExecutorService;
    }
}
