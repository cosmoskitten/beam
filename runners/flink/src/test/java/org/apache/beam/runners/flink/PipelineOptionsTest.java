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
package org.apache.beam.runners.flink;

import java.util.Collections;
import java.util.HashMap;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for serialization and deserialization of {@link PipelineOptions} in {@link DoFnOperator}.
 */
public class PipelineOptionsTest {

  /** Pipeline options. */
  public interface MyOptions extends FlinkPipelineOptions {
    @Description("Bla bla bla")
    @Default.String("Hello")
    String getTestOption();
    void setTestOption(String value);
  }

  private static MyOptions options =
      PipelineOptionsFactory.fromArgs("--testOption=nothing").as(MyOptions.class);

  @Test(expected = Exception.class)
  public void parDoBaseClassPipelineOptionsNullTest() {
    TupleTag<String> mainTag = new TupleTag<>("main-output");
    Coder<WindowedValue<String>> coder = WindowedValue.getValueOnlyCoder(StringUtf8Coder.of());
    DoFnOperator<String, String> doFnOperator = new DoFnOperator<>(
        new TestDoFn(),
        "stepName",
        coder,
        mainTag,
        Collections.<TupleTag<?>>emptyList(),
        new DoFnOperator.MultiOutputOutputManagerFactory<>(mainTag, coder),
        WindowingStrategy.globalDefault(),
        new HashMap<Integer, PCollectionView<?>>(),
        Collections.<PCollectionView<?>>emptyList(),
        null,
        null);

  }

  /**
   * Tests that PipelineOptions are present after serialization.
   */
  @Test
  public void parDoBaseClassPipelineOptionsSerializationTest() throws Exception {

    TupleTag<String> mainTag = new TupleTag<>("main-output");

    Coder<WindowedValue<String>> coder = WindowedValue.getValueOnlyCoder(StringUtf8Coder.of());
    DoFnOperator<String, String> doFnOperator = new DoFnOperator<>(
        new TestDoFn(),
        "stepName",
        coder,
        mainTag,
        Collections.<TupleTag<?>>emptyList(),
        new DoFnOperator.MultiOutputOutputManagerFactory<>(mainTag, coder),
        WindowingStrategy.globalDefault(),
        new HashMap<Integer, PCollectionView<?>>(),
        Collections.<PCollectionView<?>>emptyList(),
        options,
        null);

    final byte[] serialized = SerializationUtils.serialize(doFnOperator);

    @SuppressWarnings("unchecked")
    DoFnOperator<Object, Object> deserialized = SerializationUtils.deserialize(serialized);

    TypeInformation<WindowedValue<Object>> typeInformation = TypeInformation.of(
        new TypeHint<WindowedValue<Object>>() {});

    OneInputStreamOperatorTestHarness<WindowedValue<Object>, WindowedValue<Object>> testHarness =
        new OneInputStreamOperatorTestHarness<>(deserialized,
            typeInformation.createSerializer(new ExecutionConfig()));
    testHarness.open();

    // execute once to access options
    testHarness.processElement(new StreamRecord<>(
        WindowedValue.of(
            new Object(),
            Instant.now(),
            GlobalWindow.INSTANCE,
            PaneInfo.NO_FIRING)));

    testHarness.close();

  }

  private static class TestDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Assert.assertNotNull(c.getPipelineOptions());
      Assert.assertEquals(
          options.getTestOption(),
          c.getPipelineOptions().as(MyOptions.class).getTestOption());
    }
  }
}
