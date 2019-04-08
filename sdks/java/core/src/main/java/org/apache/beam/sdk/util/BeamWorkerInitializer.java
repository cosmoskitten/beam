/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.util;

import java.util.ServiceLoader;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/**
 * A service interface for defining one-time initialization for Beam workers.
 *
 * <p>Beam workers will use {@link ServiceLoader} to run every setup() implementation as soon as
 * possible after start up. In general this should occur immediately after logging is setup and
 * configured, but before the worker begins processing elements.
 */
@Experimental
public abstract class BeamWorkerInitializer {
  public static void runOnStartup() {
    for (BeamWorkerInitializer initializer :
        ReflectHelpers.loadServicesOrdered(BeamWorkerInitializer.class)) {
      initializer.onStartup();
    }
  }

  public static void runBeforeProcessing(PipelineOptions options) {
    for (BeamWorkerInitializer initializer :
        ReflectHelpers.loadServicesOrdered(BeamWorkerInitializer.class)) {
      initializer.beforeProcessing(options);
    }
  }

  public void onStartup() {}

  public void beforeProcessing(PipelineOptions options) {}
}
