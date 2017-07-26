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
package org.apache.beam.runners.core;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.stub.StreamObserver;
import org.apache.beam.fn.v1.BeamFnApi;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link SdkHarnessClient}. */
@RunWith(JUnit4.class)
public class SdkHarnessClientTest {

  @Mock public FnApiControlClient fnApiControlClient;

  // TODO: this should probably be a simple fake, instead of a mock,
  // and should probably have an abstraction layer above the Elements message layer
  @Mock public StreamObserver<BeamFnApi.Elements> elementReceiver;

  private SdkHarnessClient sdkHarnessClient;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    sdkHarnessClient = SdkHarnessClient.usingFnApiClients(fnApiControlClient, elementReceiver);
  }

  @Test
  public void testRegister() {
    String descriptorId1 = "descriptor1";
    String descriptorId2 = "descriptor2";

    SettableFuture<BeamFnApi.InstructionResponse> registerResponseFuture = SettableFuture.create();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(registerResponseFuture);

    sdkHarnessClient.register(
        ImmutableList.of(
            BeamFnApi.ProcessBundleDescriptor.newBuilder().setId(descriptorId1).build(),
            BeamFnApi.ProcessBundleDescriptor.newBuilder().setId(descriptorId2).build()));

    // TODO: test a meaningful property, not precise interactions
  }

  @Test
  public void testNewBundle() {
    String descriptorId1 = "descriptor1";

    SettableFuture<BeamFnApi.InstructionResponse> processBundleResponseFuture =
        SettableFuture.create();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(processBundleResponseFuture);

    SdkHarnessClient.ActiveBundle activeBundle = sdkHarnessClient.newBundle(descriptorId1);

    // TODO: test a meaningful property, not precise interactions
  }
}
