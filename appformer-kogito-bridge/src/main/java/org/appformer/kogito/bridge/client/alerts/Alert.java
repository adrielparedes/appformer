/*
 * Copyright 2020 Red Hat, Inc. and/or its affiliates.
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
 *
 *
 */

package org.appformer.kogito.bridge.client.alerts;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

@JsType(isNative = true, namespace = JsPackage.GLOBAL, name = "Object")
public class Alert {

    @JsProperty(name = "path")
    public native void setPath(String path);

    @JsProperty(name = "path")
    public native String getPath();

    @JsProperty(name = "message")
    public native void setMessage(String message);

    @JsProperty(name = "message")
    public native String getMessage();

    @JsProperty(name = "alertSeverity")
    public native void setSeverity(AlertSeverity alertSeverity);

    @JsProperty(name = "alertSeverity")
    public native AlertSeverity getSeverity();
}
