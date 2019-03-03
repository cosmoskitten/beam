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
package org.apache.beam.runners.fnexecution.jobsubmission;

import java.io.IOException;
import java.nio.file.Paths;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.core.construction.expansion.ExpansionService;
import org.apache.beam.runners.core.construction.grpc.GrpcServer;
import org.apache.beam.runners.core.construction.grpc.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.BeamFileSystemArtifactStagingService;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared code for starting and serving an {@link InMemoryJobService}. */
public abstract class JobServerDriver implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(JobServerDriver.class);

  @VisibleForTesting public ServerConfiguration configuration;

  private final ServerFactory jobServerFactory;
  private final ServerFactory artifactServerFactory;
  private final ServerFactory expansionServerFactory;

  private volatile GrpcServer<InMemoryJobService> jobServer;
  private volatile GrpcServer<BeamFileSystemArtifactStagingService> artifactStagingServer;
  private volatile GrpcServer<ExpansionService> expansionServer;

  protected abstract JobInvoker createJobInvoker();

  protected InMemoryJobService createJobService() throws IOException {
    artifactStagingServer = createArtifactStagingService();
    expansionServer = createExpansionService();

    JobInvoker invoker = createJobInvoker();
    return InMemoryJobService.create(
        artifactStagingServer.getApiServiceDescriptor(),
        this::createSessionToken,
        (String stagingSessionToken) -> {
          if (configuration.cleanArtifactsPerJob) {
            artifactStagingServer.getService().removeArtifacts(stagingSessionToken);
          }
        },
        invoker);
  }

  /** Configuration for the jobServer. */
  public static class ServerConfiguration {
    @Option(name = "--job-host", usage = "The job server host name")
    private String host = "localhost";

    @Option(
        name = "--job-port",
        usage = "The job service port. 0 to use a dynamic port. (Default: 8099)")
    private int port = 8099;

    @Option(
        name = "--artifact-port",
        usage = "The artifact service port. 0 to use a dynamic port. (Default: 8098)")
    private int artifactPort = 8098;

    @Option(
        name = "--expansion-port",
        usage = "The Java expansion service port. 0 to use a dynamic port. (Default: 8097)")
    private int expansionPort = 8097;

    @Option(name = "--artifacts-dir", usage = "The location to store staged artifact files")
    private String artifactStagingPath =
        Paths.get(System.getProperty("java.io.tmpdir"), "beam-artifact-staging").toString();

    @Option(
        name = "--clean-artifacts-per-job",
        usage = "When true, remove each job's staged artifacts when it completes")
    private boolean cleanArtifactsPerJob = false;

    @Option(
        name = "--sdk-worker-parallelism",
        usage = "Default parallelism for SDK worker processes (see portable pipeline options)")
    private Long sdkWorkerParallelism = 1L;

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    public int getArtifactPort() {
      return artifactPort;
    }

    public int getExpansionPort() {
      return expansionPort;
    }

    public String getArtifactStagingPath() {
      return artifactStagingPath;
    }

    public boolean isCleanArtifactsPerJob() {
      return cleanArtifactsPerJob;
    }

    public Long getSdkWorkerParallelism() {
      return this.sdkWorkerParallelism;
    }
  }

  protected static ServerFactory createJobServerFactory(ServerConfiguration configuration) {
    return ServerFactory.createWithPortSupplier(() -> configuration.port);
  }

  protected static ServerFactory createArtifactServerFactory(ServerConfiguration configuration) {
    return ServerFactory.createWithPortSupplier(() -> configuration.artifactPort);
  }

  protected static ServerFactory createExpansionServerFactory(ServerConfiguration configuration) {
    return ServerFactory.createWithPortSupplier(() -> configuration.expansionPort);
  }

  protected JobServerDriver(
      ServerConfiguration configuration,
      ServerFactory jobServerFactory,
      ServerFactory artifactServerFactory,
      ServerFactory expansionServerFactory) {
    this.configuration = configuration;
    this.jobServerFactory = jobServerFactory;
    this.artifactServerFactory = artifactServerFactory;
    this.expansionServerFactory = expansionServerFactory;
  }

  // This method is executed by TestPortableRunner via Reflection
  public String start() throws IOException {
    jobServer = createJobServer();
    return jobServer.getApiServiceDescriptor().getUrl();
  }

  @Override
  public void run() {
    try {
      jobServer = createJobServer();
      jobServer.getServer().awaitTermination();
    } catch (InterruptedException e) {
      LOG.warn("Job server interrupted", e);
    } catch (Exception e) {
      LOG.warn("Exception during job server creation", e);
    } finally {
      stop();
    }
  }

  // This method is executed by TestPortableRunner via Reflection
  // Needs to be synchronized to prevent concurrency issues in testing shutdown
  @SuppressWarnings("WeakerAccess")
  public synchronized void stop() {
    if (jobServer != null) {
      try {
        jobServer.close();
        LOG.info("JobServer stopped on {}", jobServer.getApiServiceDescriptor().getUrl());
        jobServer = null;
      } catch (Exception e) {
        LOG.error("Error while closing the jobServer.", e);
      }
    }
    if (artifactStagingServer != null) {
      try {
        artifactStagingServer.close();
        LOG.info(
            "ArtifactStagingServer stopped on {}",
            artifactStagingServer.getApiServiceDescriptor().getUrl());
        artifactStagingServer = null;
      } catch (Exception e) {
        LOG.error("Error while closing the artifactStagingServer.", e);
      }
    }
    if (expansionServer != null) {
      try {
        expansionServer.close();
        LOG.info("Expansion stopped on {}", expansionServer.getApiServiceDescriptor().getUrl());
        expansionServer = null;
      } catch (Exception e) {
        LOG.error("Error while closing the Expansion Service.", e);
      }
    }
  }

  protected String createSessionToken(String session) {
    return BeamFileSystemArtifactStagingService.generateStagingSessionToken(
        session, configuration.artifactStagingPath);
  }

  private GrpcServer<InMemoryJobService> createJobServer() throws IOException {
    InMemoryJobService service = createJobService();
    GrpcServer<InMemoryJobService> jobServiceGrpcServer;
    if (configuration.port == 0) {
      jobServiceGrpcServer = GrpcServer.allocatePortAndCreateFor(service, jobServerFactory);
    } else {
      Endpoints.ApiServiceDescriptor descriptor =
          Endpoints.ApiServiceDescriptor.newBuilder()
              .setUrl(configuration.host + ":" + configuration.port)
              .build();
      jobServiceGrpcServer = GrpcServer.create(service, descriptor, jobServerFactory);
    }
    LOG.info("JobService started on {}", jobServiceGrpcServer.getApiServiceDescriptor().getUrl());
    return jobServiceGrpcServer;
  }

  private GrpcServer<BeamFileSystemArtifactStagingService> createArtifactStagingService()
      throws IOException {
    BeamFileSystemArtifactStagingService service = new BeamFileSystemArtifactStagingService();
    final GrpcServer<BeamFileSystemArtifactStagingService> artifactStagingService;
    if (configuration.artifactPort == 0) {
      artifactStagingService = GrpcServer.allocatePortAndCreateFor(service, artifactServerFactory);
    } else {
      Endpoints.ApiServiceDescriptor descriptor =
          Endpoints.ApiServiceDescriptor.newBuilder()
              .setUrl(configuration.host + ":" + configuration.artifactPort)
              .build();
      artifactStagingService = GrpcServer.create(service, descriptor, artifactServerFactory);
    }
    LOG.info(
        "ArtifactStagingService started on {}",
        artifactStagingService.getApiServiceDescriptor().getUrl());
    return artifactStagingService;
  }

  private GrpcServer<ExpansionService> createExpansionService() throws IOException {
    ExpansionService service = new ExpansionService();
    GrpcServer<ExpansionService> expansionServiceGrpcServer;
    if (configuration.expansionPort == 0) {
      expansionServiceGrpcServer =
          GrpcServer.allocatePortAndCreateFor(service, expansionServerFactory);
    } else {
      Endpoints.ApiServiceDescriptor descriptor =
          Endpoints.ApiServiceDescriptor.newBuilder()
              .setUrl(configuration.host + ":" + configuration.expansionPort)
              .build();
      expansionServiceGrpcServer = GrpcServer.create(service, descriptor, expansionServerFactory);
    }
    LOG.info(
        "Java ExpansionService started on {}",
        expansionServiceGrpcServer.getApiServiceDescriptor().getUrl());
    return expansionServiceGrpcServer;
  }
}
