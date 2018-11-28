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
import net.jcip.annotations.ThreadSafe;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.runners.fnexecution.state.StateDelegator;

/** A {@link RemoteEnvironment} that connects to Dataflow runner harness. */
@ThreadSafe
public class StaticRemoteEnvironment implements RemoteEnvironment {

  static StaticRemoteEnvironment create(
      Environment environment,
      InstructionRequestHandler instructionRequestHandler,
      FnDataService beamFnDataService,
      StateDelegator beamFnStateService,
      Endpoints.ApiServiceDescriptor dataApiServiceDescriptor,
      Endpoints.ApiServiceDescriptor stateApiServiceDescriptor,
      Supplier<String> idGenerator) {
    return new StaticRemoteEnvironment(
        environment,
        instructionRequestHandler,
        beamFnDataService,
        beamFnStateService,
        dataApiServiceDescriptor,
        stateApiServiceDescriptor,
        idGenerator);
  }

  private final Object lock = new Object();
  private final Environment environment;
  private final InstructionRequestHandler instructionRequestHandler;
  private final FnDataService beamFnDataService;
  private StateDelegator beamFnStateService;
  private final Endpoints.ApiServiceDescriptor dataApiServiceDescriptor;
  private final Endpoints.ApiServiceDescriptor stateApiServiceDescriptor;
  private final Supplier<String> idGenerator;

  private boolean isClosed = false;

  private StaticRemoteEnvironment(
      Environment environment,
      InstructionRequestHandler instructionRequestHandler,
      FnDataService beamFnDataService,
      StateDelegator beamFnStateService,
      Endpoints.ApiServiceDescriptor dataApiServiceDescriptor,
      Endpoints.ApiServiceDescriptor stateApiServiceDescriptor,
      Supplier<String> idGenerator) {
    this.environment = environment;
    this.instructionRequestHandler = instructionRequestHandler;
    this.beamFnDataService = beamFnDataService;
    this.beamFnStateService = beamFnStateService;
    this.dataApiServiceDescriptor = dataApiServiceDescriptor;
    this.stateApiServiceDescriptor = stateApiServiceDescriptor;
    this.idGenerator = idGenerator;
  }

  @Override
  public Environment getEnvironment() {
    return this.environment;
  }

  @Override
  public InstructionRequestHandler getInstructionRequestHandler() {
    return this.instructionRequestHandler;
  }

  public FnDataService getBeamFnDataService() {
    return this.beamFnDataService;
  }

  public Endpoints.ApiServiceDescriptor getDataApiServiceDescriptor() {
    return this.dataApiServiceDescriptor;
  }

  public Endpoints.ApiServiceDescriptor getStateApiServiceDescriptor() {
    return this.stateApiServiceDescriptor;
  }

  public Supplier<String> getIdGenerator() {
    return this.idGenerator;
  }

  public StateDelegator getBeamFnStateService() {
    return this.beamFnStateService;
  }

  @Override
  public void close() throws Exception {
    synchronized (lock) {
      // The running docker container and instruction handler should each only be terminated once.
      // Do nothing if we have already requested termination.
      if (!isClosed) {
        isClosed = true;
        this.instructionRequestHandler.close();
      }
    }
  }
}
