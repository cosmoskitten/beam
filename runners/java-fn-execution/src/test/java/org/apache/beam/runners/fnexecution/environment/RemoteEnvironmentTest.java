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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RemoteEnvironment}. */
@RunWith(JUnit4.class)
public class RemoteEnvironmentTest {
  private AtomicBoolean closed = new AtomicBoolean(false);
  private InstructionRequestHandler handler =
      new InstructionRequestHandler() {
        @Override
        public CompletionStage<InstructionResponse> handle(InstructionRequest request) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws Exception {
          closed.set(true);
        }
      };

  @Test
  public void closeClosesInstructionRequestHandler() throws Exception {
    RemoteEnvironment env =
        new RemoteEnvironment() {
          @Override
          public Environment getEnvironment() {
            throw new UnsupportedOperationException();
          }

          @Override
          public InstructionRequestHandler getInstructionRequestHandler() {
            return handler;
          }
        };

    env.close();
    ;
    assertThat(closed.get(), is(true));
  }

  @Test
  public void forHandlerClosesHandlerOnClose() throws Exception {
    RemoteEnvironment.forHandler(Environment.getDefaultInstance(), handler).close();
    assertThat(closed.get(), is(true));
  }

  @Test
  public void forHandlerReturnsProvided() {
    Environment environment = Environment.newBuilder().setUrl("my_url").build();
    RemoteEnvironment remoteEnvironment = RemoteEnvironment.forHandler(environment, handler);
    assertThat(remoteEnvironment.getEnvironment(), equalTo(environment));
    assertThat(remoteEnvironment.getInstructionRequestHandler(), equalTo(handler));
  }
}
