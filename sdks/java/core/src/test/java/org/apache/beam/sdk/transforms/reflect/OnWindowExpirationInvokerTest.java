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
package org.apache.beam.sdk.transforms.reflect;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for invoking {@link DoFn} method annotated with
 * {@link org.apache.beam.sdk.transforms.DoFn.OnWindowExpiration} using {@link DoFnInvokers}.
 * */
@RunWith(JUnit4.class)
public class OnWindowExpirationInvokerTest {

  @Mock private DoFnInvoker.ArgumentProvider<String, String> mockArgumentProvider;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testOnWindowExpirationWithNoParameters() throws Exception {

    class SimpleTimerDoFn extends DoFn<String, String> {

      public String status = "onWindowExpiration not called";

      @ProcessElement
      public void process(ProcessContext c) {}

      @OnWindowExpiration
      public void onWindowExpiration() {
        status = "onWindowExpiration called";
      }
    }

    SimpleTimerDoFn fn = new SimpleTimerDoFn();
    DoFnInvoker<String, String> invoker = DoFnInvokers.invokerFor(fn);

    invoker.invokeOnWindowExpiration(mockArgumentProvider);
    assertThat(fn.status, equalTo("onWindowExpiration called"));
  }
}
