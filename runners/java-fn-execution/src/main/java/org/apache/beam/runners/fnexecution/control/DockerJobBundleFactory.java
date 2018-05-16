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
import java.time.Duration;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.ActiveBundle;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.BundleProcessor;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.DockerCommand;
import org.apache.beam.runners.fnexecution.environment.DockerEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;
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

  private final GrpcFnServer<FnApiControlClientPoolService> controlServer;
  private final GrpcFnServer<GrpcLoggingService> loggingServer;
  private final GrpcFnServer<ArtifactRetrievalService> retrievalServer;
  private final GrpcFnServer<StaticGrpcProvisionService> provisioningServer;

  private final LoadingCache<Environment, WrappedSdkHarnessClient> environmentCache;

  public static DockerJobBundleFactory create() throws Exception {
    DockerCommand dockerCommand = DockerCommand.forExecutable("docker", Duration.ofSeconds(60));
    // TODO: Should we create our own services here or have them be provided?
    GrpcFnServer<FnApiControlClientPoolService> controlServer =
        GrpcFnServer.allocatePortAndCreateFor(null, null);
    GrpcFnServer<GrpcLoggingService> loggingServer =
        GrpcFnServer.allocatePortAndCreateFor(null, null);
    GrpcFnServer<ArtifactRetrievalService> retrievalServer =
        GrpcFnServer.allocatePortAndCreateFor(null, null);
    GrpcFnServer<StaticGrpcProvisionService> provisioningServer =
        GrpcFnServer.allocatePortAndCreateFor(null, null);
    DockerEnvironmentFactory environmentFactory =
        DockerEnvironmentFactory.forServices(
            dockerCommand,
            controlServer,
            loggingServer,
            retrievalServer,
            provisioningServer,
            null,
            null);
    return new DockerJobBundleFactory(
        environmentFactory, controlServer, loggingServer, retrievalServer, provisioningServer);
  }

  private DockerJobBundleFactory(
      DockerEnvironmentFactory environmentFactory,
      GrpcFnServer<FnApiControlClientPoolService> controlServer,
      GrpcFnServer<GrpcLoggingService> loggingServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServer) {
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
                    return WrappedSdkHarnessClient.wrapping(remoteEnvironment);
                  }
                });
  }

  @Override
  public <T> StageBundleFactory<T> forStage(ExecutableStage executableStage) {
    WrappedSdkHarnessClient wrappedClient =
        environmentCache.getUnchecked(executableStage.getEnvironment());
    return SimpleStageBundleFactory.create(wrappedClient);
  }

  @Override
  public void close() throws Exception {
    environmentCache.invalidateAll();
    environmentCache.cleanUp();
    controlServer.close();
    loggingServer.close();
    retrievalServer.close();
    provisioningServer.close();
  }

  private static class SimpleStageBundleFactory<InputT> implements StageBundleFactory<InputT> {

    private final BundleProcessor<InputT> processor;

    private WrappedSdkHarnessClient wrappedClient;

    static <InputT> SimpleStageBundleFactory<InputT> create(WrappedSdkHarnessClient wrappedClient) {
      BundleProcessor<InputT> processor =
          wrappedClient.getClient().getProcessor(null, null, wrappedClient.getStateService());
      return new SimpleStageBundleFactory<InputT>(processor, wrappedClient);
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
      ActiveBundle<InputT> bundle = processor.newBundle(null, stateRequestHandler);
      // TODO: Should ActiveBundle implement RemoteBundle?
      return new RemoteBundle<InputT>() {
        @Override
        public String getId() {
          return bundle.getId();
        }

        @Override
        public FnDataReceiver<WindowedValue<InputT>> getInputReceiver() {
          return bundle.getInputReceiver();
        }

        @Override
        public void close() throws Exception {
          bundle.close();
        }
      };
    }

    @Override
    public void close() throws Exception {
      // Clear reference to encourage cache eviction. Values are weakly referenced.
      wrappedClient = null;
    }
  }

  private static class WrappedSdkHarnessClient implements AutoCloseable {
    private final RemoteEnvironment environment;
    // TODO: This data service should probably not live here. It is necessary for now because
    // SdkHarnessClient requires one at construction.
    private final GrpcFnServer<GrpcDataService> dataServer;
    private final GrpcFnServer<GrpcStateService> stateServer;
    private final SdkHarnessClient client;

    static WrappedSdkHarnessClient wrapping(RemoteEnvironment environment) throws Exception {
      GrpcFnServer<GrpcDataService> dataServer = GrpcFnServer.allocatePortAndCreateFor(null, null);
      GrpcFnServer<GrpcStateService> stateServer =
          GrpcFnServer.allocatePortAndCreateFor(null, null);
      SdkHarnessClient client =
          SdkHarnessClient.usingFnApiClient(
              environment.getInstructionRequestHandler(), dataServer.getService());
      return new WrappedSdkHarnessClient(environment, dataServer, stateServer, client);
    }

    private WrappedSdkHarnessClient(
        RemoteEnvironment environment,
        GrpcFnServer<GrpcDataService> dataServer,
        GrpcFnServer<GrpcStateService> stateServer,
        SdkHarnessClient client) {
      this.environment = environment;
      this.dataServer = dataServer;
      this.stateServer = stateServer;
      this.client = client;
    }

    SdkHarnessClient getClient() {
      return client;
    }

    GrpcStateService getStateService() {
      return stateServer.getService();
    }

    @Override
    public void close() throws Exception {
      environment.close();
      dataServer.close();
      stateServer.close();
    }
  }
}
