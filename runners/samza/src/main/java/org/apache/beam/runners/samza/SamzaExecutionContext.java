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
package org.apache.beam.runners.samza;

import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.samza.metrics.SamzaMetricsContainer;

/** Runtime context for the Samza runner. */
public class SamzaExecutionContext {
  private SamzaMetricsContainer metricsContainer;
  private JobBundleFactory jobBundleFactory;

  public SamzaMetricsContainer getMetricsContainer() {
    return this.metricsContainer;
  }

  void setMetricsContainer(SamzaMetricsContainer metricsContainer) {
    this.metricsContainer = metricsContainer;
  }

  public JobBundleFactory getJobBundleFactory() {
    return this.jobBundleFactory;
  }

  void setJobBundleFactory(JobBundleFactory jobBundleFactory) {
    this.jobBundleFactory = jobBundleFactory;
  }
}
