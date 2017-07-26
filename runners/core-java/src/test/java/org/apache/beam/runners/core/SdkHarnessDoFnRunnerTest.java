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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link SdkHarnessDoFnRunner}. */
@RunWith(JUnit4.class)
public class SdkHarnessDoFnRunnerTest {
  @Mock private SdkHarnessClient mockClient;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testStartAndFinishBundle() {
    String processBundleDescriptorId = "testBundle";
    Coder<WindowedValue<Void>> inputCoder =
        WindowedValue.getFullCoder(VoidCoder.of(), IntervalWindow.getCoder());
    SdkHarnessDoFnRunner<Void, Void> underTest =
        SdkHarnessDoFnRunner.<Void, Void>create(mockClient, processBundleDescriptorId);

    underTest.startBundle();
    underTest.finishBundle();

    // TODO: test meaningful properties
  }
}
