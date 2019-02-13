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
package org.apache.beam.runners.flink.streaming;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.nio.ByteBuffer;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaceForTest;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkKeyGroupStateInternals;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link FlinkKeyGroupStateInternals}. This is based on the tests for {@code
 * StateInternalsTest}.
 */
public class FlinkKeyGroupStateInternalsTest {

  private static final StateNamespace NAMESPACE_1 = new StateNamespaceForTest("ns1");
  private static final StateNamespace NAMESPACE_2 = new StateNamespaceForTest("ns2");

  private static final StateTag<BagState<String>> STRING_BAG_ADDR =
      StateTags.bag("stringBag", StringUtf8Coder.of());

  private StateInternals stateInternals;

  @Before
  public void createStateInternals() {
    KeyedStateBackend keyedStateBackend = getKeyedStateBackend(2, new KeyGroupRange(0, 1));
    stateInternals = new FlinkKeyGroupStateInternals<>(StringUtf8Coder.of(), keyedStateBackend);
  }

  @Test
  public void testBag() throws Exception {
    BagState<String> bagState1 = stateInternals.state(NAMESPACE_1, STRING_BAG_ADDR);
    BagState<String> bagState2 = stateInternals.state(NAMESPACE_2, STRING_BAG_ADDR);

    assertThat(bagState1.read(), Matchers.emptyIterable());
    assertThat(bagState2.read(), Matchers.emptyIterable());

    bagState1.add("hello");
    bagState2.add("hello");
    bagState1.add("world");
    bagState2.add("world");
    assertThat(bagState1.read(), containsInAnyOrder("hello", "world"));
    assertThat(bagState2.read(), containsInAnyOrder("hello", "world"));

    assertThat(bagState1.read(), Matchers.emptyIterable());
    assertThat(bagState2.read(), Matchers.emptyIterable());
  }

  @Test
  public void testBagClear() throws Exception {
    BagState<String> bagState = stateInternals.state(NAMESPACE_1, STRING_BAG_ADDR);
    try {
      bagState.clear();
    } catch (UnsupportedOperationException e) {
      // this is what we want
    }
  }

  @Test
  public void testBagIsEmpty() throws Exception {
    BagState<String> value = stateInternals.state(NAMESPACE_1, STRING_BAG_ADDR);
    try {
      value.isEmpty();
    } catch (UnsupportedOperationException e) {
      // this is what we want
    }
  }

  private static KeyedStateBackend<ByteBuffer> getKeyedStateBackend(
      int numberOfKeyGroups, KeyGroupRange keyGroupRange) {
    MemoryStateBackend backend = new MemoryStateBackend();
    try {
      AbstractKeyedStateBackend<ByteBuffer> keyedStateBackend =
          backend.createKeyedStateBackend(
              new DummyEnvironment("test", 1, 0),
              new JobID(),
              "test_op",
              new GenericTypeInfo<>(ByteBuffer.class).createSerializer(new ExecutionConfig()),
              numberOfKeyGroups,
              keyGroupRange,
              new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()));
      keyedStateBackend.setCurrentKey(
          ByteBuffer.wrap(CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "1")));
      return keyedStateBackend;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
