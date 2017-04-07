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

package org.apache.beam.sdk.util;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.WindowingStrategy.AccumulationMode;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SdkComponents}. */
@RunWith(JUnit4.class)
public class SdkComponentsTest {
  @Rule
  public TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private SdkComponents components = SdkComponents.create();

  @Test
  public void putThenGetCoder() {
    Coder<?> coder =
        KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(SetCoder.of(ByteArrayCoder.of())));
    String name = components.putCoder(coder);
    String nameFromGet = components.getCoderId(coder);
    assertThat(name, equalTo(nameFromGet));
  }

  @Test
  public void getCoderNotPresent() {
    Coder<?> coder =
        KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(SetCoder.of(ByteArrayCoder.of())));
    thrown.expect(IllegalArgumentException.class);
    components.getCoderId(coder);
  }

  @Test
  public void putThenGetTransform() {
    Create.Values<Integer> create = Create.of(1, 2, 3);
    PCollection<Integer> pt = pipeline.apply(create);
    String userName = "my_transform/my_nesting";
    AppliedPTransform<?, ?, ?> transform =
        AppliedPTransform.<PBegin, PCollection<Integer>, Create.Values<Integer>>of(
            userName, pipeline.begin().expand(), pt.expand(), create, pipeline);
    String componentName = components.putTransform(transform);
    assertThat(componentName, equalTo(userName));
    assertThat(components.getTransformId(transform), equalTo(componentName));
  }

  @Test
  public void putThenGetTransformEmptyFullName() {
    Create.Values<Integer> create = Create.of(1, 2, 3);
    PCollection<Integer> pt = pipeline.apply(create);
    AppliedPTransform<?, ?, ?> transform =
        AppliedPTransform.<PBegin, PCollection<Integer>, Create.Values<Integer>>of(
            "", pipeline.begin().expand(), pt.expand(), create, pipeline);
    String assignedName = components.putTransform(transform);
    String retrievedName = components.getTransformId(transform);

    assertThat(assignedName, not(nullValue()));
    assertThat(assignedName, not(isEmptyString()));
    assertThat(assignedName, equalTo(retrievedName));
  }

  @Test
  public void getPTransformNotPresent() {
    Create.Values<Integer> create = Create.of(1, 2, 3);
    PCollection<Integer> pt = pipeline.apply(create);
    String userName = "my_transform/my_nesting";
    AppliedPTransform<?, ?, ?> transform =
        AppliedPTransform.<PBegin, PCollection<Integer>, Create.Values<Integer>>of(
            userName, pipeline.begin().expand(), pt.expand(), create, pipeline);
    thrown.expect(IllegalArgumentException.class);
    components.getTransformId(transform);
  }

  @Test
  public void putThenGetPCollection() {
    PCollection<Long> pCollection = pipeline.apply(CountingInput.unbounded());
    String assignedId = "foo";
    String id = components.putPCollection(pCollection, assignedId);
    assertThat(id, equalTo(assignedId));
    assertThat(components.getPCollectionId(pCollection), equalTo(id));
  }

  @Test
  public void putPCollectionNewIdAlreadyExists() {
    PCollection<Long> pCollection = pipeline.apply(CountingInput.unbounded());
    String assignedId = "foo";
    String id = components.putPCollection(pCollection, assignedId);
    String reinserted = components.putPCollection(pCollection, "bar");
    assertThat(reinserted, equalTo(id));
    assertThat(components.getPCollectionId(pCollection), equalTo(assignedId));
  }

  @Test
  public void putPCollectionExistingNameCollision() {
    PCollection<Long> pCollection = pipeline.apply("FirstCount", CountingInput.unbounded());
    String assignedId = "foo";
    components.putPCollection(pCollection, assignedId);
    PCollection<Long> duplicate = pipeline.apply("SecondCount", CountingInput.unbounded());
    thrown.expect(IllegalArgumentException.class);
    components.putPCollection(duplicate, assignedId);
  }

  @Test
  public void getPCollectionNotPresent() {
    PCollection<Long> pCollection = pipeline.apply(CountingInput.unbounded());
    thrown.expect(IllegalArgumentException.class);
    components.getPCollectionId(pCollection);
  }

  @Test
  public void putThenGetWindowingStrategy() {
    WindowingStrategy<?, ?> strategy =
        WindowingStrategy.globalDefault().withMode(AccumulationMode.ACCUMULATING_FIRED_PANES);
    String name = components.putWindowingStrategy(strategy);
    String retrieved = components.getWindowingStrategyId(strategy);
    assertThat(name, not(isEmptyOrNullString()));
    assertThat(name, equalTo(retrieved));
  }

  // TODO: Determine if desired
  // @Test public void windowingStrategyGlobalDefault()

  @Test
  public void putWindowingStrategiesEqual() {
    WindowingStrategy<?, ?> strategy =
        WindowingStrategy.globalDefault().withMode(AccumulationMode.ACCUMULATING_FIRED_PANES);
    String name = components.putWindowingStrategy(strategy);
    String duplicateName =
        components.putWindowingStrategy(
            WindowingStrategy.globalDefault().withMode(AccumulationMode.ACCUMULATING_FIRED_PANES));
    assertThat(name, equalTo(duplicateName));
  }

  @Test
  public void getWindowingStrategyNotPresent() {
    WindowingStrategy<?, ?> strategy =
        WindowingStrategy.globalDefault().withMode(AccumulationMode.ACCUMULATING_FIRED_PANES);
    thrown.expect(IllegalArgumentException.class);
    components.getWindowingStrategyId(strategy);
  }
}
