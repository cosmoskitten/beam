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

package org.apache.beam.runners.fnexecution.manager;

import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;

/**
 * A manager of resources related to the SDK Harness, capable of providing RemoteBundles to runner
 * operators.
 *
 * <p>In order to provide a simple interface to runner operators, the SdkHarnessManager is
 * responsible for owning and managing the lifetimes of resources such as RPC servers and remote
 * environments. It is responsible for both instantiation and cleanup of these resources.  Since all
 * managed resources are owned by the SdkHarnessManager, it is responsible for cleaning them up when
 * its close function is called.
 */
public interface SdkHarnessManager extends AutoCloseable {
  /**
   * Register a {@link ProvisionApi.ProvisionInfo} for a job.
   */
  void registerJobInfo(ProvisionApi.ProvisionInfo jobInfo);


  /**
   * Register an {@link ArtifactSource} as available for a job.
   * @param jobId ID for the job the {@link ArtifactSource} belongs to.
   * @param artifactSource The {@link ArtifactSource} to register.
   * @return A closeable handle notifying the manager that the {@link ArtifactSource} is no longer
   * available.
   */
  AutoCloseable registerArtifactSource(String jobId, ArtifactSource artifactSource);

  /**
   * Register a {@link StateRequestHandler} as available for a job.
   * @param jobId ID for the job the {@link ArtifactSource} belongs to.
   * @param stateRequestHandler The {@link StateRequestHandler} to register.
   * @return A closeable handle notifying the manager that the {@link StateRequestHandler} is no
   * longer available.
   */
  AutoCloseable registerStateRequestHandler(String jobId, StateRequestHandler stateRequestHandler);

  /**
   * Get a new {@link RemoteBundle bundle} for processing the data in an executable stage.
   *
   * <p>If necessary, this blocks while provisioning the remote resources necessary to support
   * bundle processing.
   */
  <InputT> RemoteBundle<InputT> getBundle(
      String jobId, ExecutableStage executableStage) throws Exception;
}
