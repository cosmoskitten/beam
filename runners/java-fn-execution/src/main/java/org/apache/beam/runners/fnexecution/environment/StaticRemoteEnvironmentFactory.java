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

import java.util.function.Supplier;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.sdk.fn.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticRemoteEnvironmentFactory implements EnvironmentFactory {

  private static final Logger LOG = LoggerFactory.getLogger(StaticRemoteEnvironmentFactory.class);

  static StaticRemoteEnvironmentFactory forService(
      InstructionRequestHandler instructionRequestHandler,
      FnDataService beamFnDataService,
      StateDelegator beamFnStateService,
      Endpoints.ApiServiceDescriptor dataApiServiceDescriptor,
      Endpoints.ApiServiceDescriptor stateApiServiceDescriptor,
      Supplier<String> idGenerator) {
    StaticRemoteEnvironmentFactory factory = new StaticRemoteEnvironmentFactory();
    factory.setStaticServiceContent(
        instructionRequestHandler,
        beamFnDataService,
        beamFnStateService,
        dataApiServiceDescriptor,
        stateApiServiceDescriptor,
        idGenerator);
    return factory;
  }

  private InstructionRequestHandler instructionRequestHandler;
  private FnDataService beamFnDataService;
  private StateDelegator beamFnStateService;
  private Endpoints.ApiServiceDescriptor dataApiServiceDescriptor;
  private Endpoints.ApiServiceDescriptor stateApiServiceDescriptor;
  private Supplier<String> idGenerator;

  private void setStaticServiceContent(
      InstructionRequestHandler instructionRequestHandler,
      FnDataService beamFnDataService,
      StateDelegator beamFnStateService,
      Endpoints.ApiServiceDescriptor dataApiServiceDescriptor,
      Endpoints.ApiServiceDescriptor stateApiServiceDescriptor,
      Supplier<String> idGenerator) {
    this.instructionRequestHandler = instructionRequestHandler;
    this.beamFnDataService = beamFnDataService;
    this.beamFnStateService = beamFnStateService;
    this.dataApiServiceDescriptor = dataApiServiceDescriptor;
    this.stateApiServiceDescriptor = stateApiServiceDescriptor;
    this.idGenerator = idGenerator;
  }

  @Override
  public RemoteEnvironment createEnvironment(Environment environment) {
    return StaticRemoteEnvironment.create(
        environment,
        this.instructionRequestHandler,
        this.beamFnDataService,
        this.beamFnStateService,
        this.dataApiServiceDescriptor,
        this.stateApiServiceDescriptor,
        this.idGenerator);
  }

  public static class Provider implements EnvironmentFactory.Provider {
    private final InstructionRequestHandler instructionRequestHandler;
    private final FnDataService beamFnDataService;
    private StateDelegator beamFnStateService;
    private Endpoints.ApiServiceDescriptor dataApiServiceDescriptor;
    private Endpoints.ApiServiceDescriptor stateApiServiceDescriptor;
    private Supplier<String> idGenerator;

    public Provider(
        InstructionRequestHandler instructionRequestHandler,
        FnDataService beamFnDataService,
        StateDelegator beamFnStateService,
        Endpoints.ApiServiceDescriptor dataApiServiceDescriptor,
        Endpoints.ApiServiceDescriptor stateApiServiceDescriptor,
        Supplier<String> idGenerator) {
      this.instructionRequestHandler = instructionRequestHandler;
      this.beamFnDataService = beamFnDataService;
      this.beamFnStateService = beamFnStateService;
      this.dataApiServiceDescriptor = dataApiServiceDescriptor;
      this.stateApiServiceDescriptor = stateApiServiceDescriptor;
      this.idGenerator = idGenerator;
    }

    @Override
    public EnvironmentFactory createEnvironmentFactory(
        GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
        GrpcFnServer<GrpcLoggingService> loggingServiceServer,
        GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
        GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
        ControlClientPool clientPool,
        IdGenerator idGenerator) {
      return StaticRemoteEnvironmentFactory.forService(
          this.instructionRequestHandler,
          this.beamFnDataService,
          this.beamFnStateService,
          this.dataApiServiceDescriptor,
          this.stateApiServiceDescriptor,
          this.idGenerator);
    }
  }
}
