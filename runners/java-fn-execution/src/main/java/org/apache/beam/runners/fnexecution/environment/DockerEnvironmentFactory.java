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
package org.apache.beam.runners.fnexecution.environment;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ControlClientSource;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;

/**
 * An {@link EnvironmentFactory} that creates docker containers by shelling out to docker. Not
 * thread-safe.
 */
public class DockerEnvironmentFactory implements EnvironmentFactory {

  public static DockerEnvironmentFactory forServices(
      DockerWrapper docker,
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      ControlClientSource clientSource,
      // TODO: Refine this to IdGenerator when we determine where that should live.
      Supplier<String> idGenerator) {
    return new DockerEnvironmentFactory(
        docker,
        controlServiceServer,
        loggingServiceServer,
        retrievalServiceServer,
        provisioningServiceServer,
        idGenerator,
        clientSource);
  }

  private final DockerWrapper docker;
  private final GrpcFnServer<FnApiControlClientPoolService> controlServiceServer;
  private final GrpcFnServer<GrpcLoggingService> loggingServiceServer;
  private final GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer;
  private final GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;
  private final Supplier<String> idGenerator;
  private final ControlClientSource clientSource;

  private RemoteEnvironment dockerEnvironment = null;

  private DockerEnvironmentFactory(
      DockerWrapper docker,
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      Supplier<String> idGenerator,
      ControlClientSource clientSource) {
    this.docker = docker;
    this.controlServiceServer = controlServiceServer;
    this.loggingServiceServer = loggingServiceServer;
    this.retrievalServiceServer = retrievalServiceServer;
    this.provisioningServiceServer = provisioningServiceServer;
    this.idGenerator = idGenerator;
    this.clientSource = clientSource;
  }

  /** Creates an active {@link RemoteEnvironment} backed by a local Docker container. */
  @Override
  public RemoteEnvironment createEnvironment(Environment environment) throws Exception {
    if (dockerEnvironment == null) {
      dockerEnvironment = createDockerEnv(environment);
    } else {
      checkArgument(
          environment.getUrl().equals(dockerEnvironment.getEnvironment().getUrl()),
          "A %s must only be queried for a single %s. Existing %s, Argument %s",
          DockerEnvironmentFactory.class.getSimpleName(),
          Environment.class.getSimpleName(),
          dockerEnvironment.getEnvironment().getUrl(),
          environment.getUrl());
    }
    return dockerEnvironment;
  }

  private DockerContainerEnvironment createDockerEnv(Environment environment) throws Exception {
    String workerId = idGenerator.get();

    // Prepare docker invocation.
    Path workerPersistentDirectory = Files.createTempDirectory("worker_persistent_directory");
    Path semiPersistentDirectory = Files.createTempDirectory("semi_persistent_dir");
    String containerImage = environment.getUrl();
    // TODO: The default service address will not work for Docker for Mac.
    String loggingEndpoint = loggingServiceServer.getApiServiceDescriptor().getUrl();
    String artifactEndpoint = retrievalServiceServer.getApiServiceDescriptor().getUrl();
    String provisionEndpoint = provisioningServiceServer.getApiServiceDescriptor().getUrl();
    String controlEndpoint = controlServiceServer.getApiServiceDescriptor().getUrl();
    List<String> args =
        Arrays.asList(
            "-v",
            // TODO: Mac only allows temporary mounts under /tmp by default (as of 17.12).
            String.format("%s:%s", workerPersistentDirectory, semiPersistentDirectory),
            // NOTE: Host networking does not work on Mac, but the command line flag is accepted.
            "--network=host",
            containerImage,
            String.format("--id=%s", workerId),
            String.format("--logging_endpoint=%s", loggingEndpoint),
            String.format("--artifact_endpoint=%s", artifactEndpoint),
            String.format("--provision_endpoint=%s", provisionEndpoint),
            String.format("--control_endpoint=%s", controlEndpoint),
            String.format("--semi_persist_dir=%s", semiPersistentDirectory));

    // Wrap the blocking call to clientSource.get in case an exception is thrown.
    String containerId = null;
    InstructionRequestHandler instructionHandler;
    try {
      containerId = docker.runImage(containerImage, args);
      // Wait on a client from the gRPC server.
      instructionHandler = clientSource.get(workerId);
    } catch (Exception e) {
      if (containerId != null) {
        // Kill the launched docker container if we can't retrieve a client for it.
        try {
          docker.killContainer(containerId);
        } catch (Exception dockerException) {
          e.addSuppressed(dockerException);
        }
      }
      throw e;
    }

    return DockerContainerEnvironment.create(docker, environment, containerId, instructionHandler);
  }
}
