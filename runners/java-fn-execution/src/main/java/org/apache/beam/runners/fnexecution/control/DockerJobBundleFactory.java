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
package org.apache.beam.runners.fnexecution.control;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.model.fnexecution.v1.ProvisionApi.ProvisionInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.GrpcContextHeaderAccessorProvider;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.BundleProcessor;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.environment.DockerCommand;
import org.apache.beam.runners.fnexecution.environment.DockerEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.Slf4jLogWriter;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link JobBundleFactory} that uses a {@link DockerEnvironmentFactory} for environment
 * management. Note that returned {@link StageBundleFactory stage bundle factories} are not
 * thread-safe. Instead, a new stage factory should be created for each client.
 */
@ThreadSafe
public class DockerJobBundleFactory implements JobBundleFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DockerJobBundleFactory.class);

  private final ServerFactory serverFactory;
  private final GrpcFnServer<FnApiControlClientPoolService> controlServer;
  private final GrpcFnServer<GrpcLoggingService> loggingServer;
  private final GrpcFnServer<ArtifactRetrievalService> retrievalServer;
  private final GrpcFnServer<StaticGrpcProvisionService> provisioningServer;

  private final LoadingCache<Environment, WrappedSdkHarnessClient> environmentCache;

  public static DockerJobBundleFactory create() throws Exception {
    DockerCommand dockerCommand = DockerCommand.forExecutable("docker", Duration.ofSeconds(60));
    // TODO: Use ServerFactory that produces correct service descriptors for docker-on-mac.
    ServerFactory serverFactory = ServerFactory.createDefault();
    ControlClientPool clientPool = MapControlClientPool.create();

    GrpcFnServer<FnApiControlClientPoolService> controlServer =
        GrpcFnServer.allocatePortAndCreateFor(
            FnApiControlClientPoolService.offeringClientsToPool(
                clientPool.getSink(), GrpcContextHeaderAccessorProvider.getHeaderAccessor()),
            serverFactory);
    GrpcFnServer<GrpcLoggingService> loggingServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcLoggingService.forWriter(Slf4jLogWriter.getDefault()), serverFactory);
    // TODO: Wire in artifact retrieval service once implemented.
    GrpcFnServer<ArtifactRetrievalService> retrievalServer =
        GrpcFnServer.allocatePortAndCreateFor(null, serverFactory);
    GrpcFnServer<StaticGrpcProvisionService> provisioningServer =
        GrpcFnServer.allocatePortAndCreateFor(
            StaticGrpcProvisionService.create(ProvisionInfo.newBuilder().build()), serverFactory);
    // TODO: Wire in IdGenerators when available.
    DockerEnvironmentFactory environmentFactory =
        DockerEnvironmentFactory.forServices(
            dockerCommand,
            controlServer,
            loggingServer,
            retrievalServer,
            provisioningServer,
            clientPool.getSource(),
            null);
    return new DockerJobBundleFactory(
        environmentFactory,
        serverFactory,
        controlServer,
        loggingServer,
        retrievalServer,
        provisioningServer);
  }

  private DockerJobBundleFactory(
      DockerEnvironmentFactory environmentFactory,
      ServerFactory serverFactory,
      GrpcFnServer<FnApiControlClientPoolService> controlServer,
      GrpcFnServer<GrpcLoggingService> loggingServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServer) {
    this.serverFactory = serverFactory;
    this.controlServer = controlServer;
    this.loggingServer = loggingServer;
    this.retrievalServer = retrievalServer;
    this.provisioningServer = provisioningServer;
    this.environmentCache =
        CacheBuilder.newBuilder()
            .weakValues()
            .removalListener(
                ((RemovalNotification<Environment, WrappedSdkHarnessClient> notification) -> {
                  LOG.debug("Cleaning up for environment {}", notification.getKey().getUrl());
                  try {
                    notification.getValue().close();
                  } catch (Exception e) {
                    LOG.warn("Error cleaning up environment", notification.getKey(), e);
                  }
                }))
            .build(
                new CacheLoader<Environment, WrappedSdkHarnessClient>() {
                  @Override
                  public WrappedSdkHarnessClient load(Environment environment) throws Exception {
                    RemoteEnvironment remoteEnvironment =
                        environmentFactory.createEnvironment(environment);
                    return WrappedSdkHarnessClient.wrapping(remoteEnvironment, serverFactory);
                  }
                });
  }

  @Override
  public <T> StageBundleFactory<T> forStage(ExecutableStage executableStage) {
    WrappedSdkHarnessClient wrappedClient =
        environmentCache.getUnchecked(executableStage.getEnvironment());
    ExecutableProcessBundleDescriptor processBundleDescriptor;
    try {
      processBundleDescriptor =
          ProcessBundleDescriptors.fromExecutableStage(
              "id", executableStage, wrappedClient.getDataServer().getApiServiceDescriptor());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return SimpleStageBundleFactory.create(wrappedClient, processBundleDescriptor);
  }

  @Override
  public void close() throws Exception {
    // Clear the cache. This closes all active environments.
    environmentCache.invalidateAll();
    environmentCache.cleanUp();

    // Tear down common servers.
    controlServer.close();
    loggingServer.close();
    retrievalServer.close();
    provisioningServer.close();
  }

  private static class SimpleStageBundleFactory<InputT> implements StageBundleFactory<InputT> {

    private final BundleProcessor<InputT> processor;

    // Store the wrapped client in order to keep a live reference into the cache.
    @SuppressFBWarnings private WrappedSdkHarnessClient wrappedClient;

    static <InputT> SimpleStageBundleFactory<InputT> create(
        WrappedSdkHarnessClient wrappedClient,
        ExecutableProcessBundleDescriptor processBundleDescriptor) {
      @SuppressWarnings("unchecked")
      BundleProcessor<InputT> processor =
          wrappedClient
              .getClient()
              .getProcessor(
                  processBundleDescriptor.getProcessBundleDescriptor(),
                  (RemoteInputDestination) processBundleDescriptor.getRemoteInputDestination(),
                  wrappedClient.getStateServer().getService());
      return new SimpleStageBundleFactory<>(processor, wrappedClient);
    }

    SimpleStageBundleFactory(
        BundleProcessor<InputT> processor, WrappedSdkHarnessClient wrappedClient) {
      this.processor = processor;
      this.wrappedClient = wrappedClient;
    }

    @Override
    public RemoteBundle<InputT> getBundle(
        OutputReceiverFactory outputReceiverFactory, StateRequestHandler stateRequestHandler)
        throws Exception {
      // TODO: We cannot construct an output receiver map from an output receiver factory. Should
      // newBundle take an OutputReceiverFactory as an argument?
      return processor.newBundle(Collections.emptyMap(), stateRequestHandler);
    }

    @Override
    public void close() throws Exception {
      // Clear reference to encourage cache eviction. Values are weakly referenced.
      wrappedClient = null;
    }
  }

  private static class WrappedSdkHarnessClient implements AutoCloseable {
    private final RemoteEnvironment environment;
    private final ExecutorService executor;
    // TODO: This data service should probably not live here. It is necessary for now because
    // SdkHarnessClient requires one at construction.
    private final GrpcFnServer<GrpcDataService> dataServer;
    private final GrpcFnServer<GrpcStateService> stateServer;
    private final SdkHarnessClient client;

    static WrappedSdkHarnessClient wrapping(
        RemoteEnvironment environment, ServerFactory serverFactory) throws Exception {
      ExecutorService executor = Executors.newCachedThreadPool();
      GrpcFnServer<GrpcDataService> dataServer =
          GrpcFnServer.allocatePortAndCreateFor(GrpcDataService.create(executor), serverFactory);
      GrpcFnServer<GrpcStateService> stateServer =
          GrpcFnServer.allocatePortAndCreateFor(GrpcStateService.create(), serverFactory);
      SdkHarnessClient client =
          SdkHarnessClient.usingFnApiClient(
              environment.getInstructionRequestHandler(), dataServer.getService());
      return new WrappedSdkHarnessClient(environment, executor, dataServer, stateServer, client);
    }

    private WrappedSdkHarnessClient(
        RemoteEnvironment environment,
        ExecutorService executor,
        GrpcFnServer<GrpcDataService> dataServer,
        GrpcFnServer<GrpcStateService> stateServer,
        SdkHarnessClient client) {
      this.executor = executor;
      this.environment = environment;
      this.dataServer = dataServer;
      this.stateServer = stateServer;
      this.client = client;
    }

    SdkHarnessClient getClient() {
      return client;
    }

    GrpcFnServer<GrpcStateService> getStateServer() {
      return stateServer;
    }

    GrpcFnServer<GrpcDataService> getDataServer() {
      return dataServer;
    }

    @Override
    public void close() throws Exception {
      executor.shutdown();
      // TODO: Wait for shutdown?
      environment.close();
      dataServer.close();
      stateServer.close();
    }
  }
}
