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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedSourceWrapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for {@link UnboundedSourceWrapper}.
 */
@RunWith(Enclosed.class)
public class UnboundedSourceWrapperTest {

  /**
   * Parameterized tests.
   */
  @RunWith(Parameterized.class)
  public static class UnboundedSourceWrapperTestWithParams {
    private final int numTasks;
    private final int numSplits;

    public UnboundedSourceWrapperTestWithParams(int numTasks, int numSplits) {
      this.numTasks = numTasks;
      this.numSplits = numSplits;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
      /*
       * Parameters for initializing the tests:
       * {numTasks, numSplits}
       * The test currently assumes powers of two for some assertions.
       */
      return Arrays.asList(new Object[][]{
          {1, 1}, {1, 2}, {1, 4},
          {2, 1}, {2, 2}, {2, 4},
          {4, 1}, {4, 2}, {4, 4}
      });
    }

    /**
     * Creates a {@link UnboundedSourceWrapper} that has one or multiple readers per source.
     * If numSplits > numTasks the source has one source will manage multiple readers.
     */
    @Test
    public void testReaders() throws Exception {
      final int numElements = 20;
      final Object checkpointLock = new Object();
      PipelineOptions options = PipelineOptionsFactory.create();

      // this source will emit exactly NUM_ELEMENTS across all parallel readers,
      // afterwards it will stall. We check whether we also receive NUM_ELEMENTS
      // elements later.
      TestCountingSource source = new TestCountingSource(numElements);
      UnboundedSourceWrapper<KV<Integer, Integer>, TestCountingSource.CounterMark> flinkWrapper =
          new UnboundedSourceWrapper<>(options, source, numSplits);

      assertEquals(numSplits, flinkWrapper.getSplitSources().size());

      StreamSource<WindowedValue<
          KV<Integer, Integer>>,
          UnboundedSourceWrapper<
              KV<Integer, Integer>,
              TestCountingSource.CounterMark>> sourceOperator = new StreamSource<>(flinkWrapper);

      AbstractStreamOperatorTestHarness<WindowedValue<KV<Integer, Integer>>> testHarness =
          new AbstractStreamOperatorTestHarness<>(
              sourceOperator,
              numTasks /* parallelism */,
              numTasks /* max parallelism */,
              0 /* subtask index */);

      testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);

      try {
        testHarness.open();
        sourceOperator.run(checkpointLock,
            new Output<StreamRecord<WindowedValue<KV<Integer, Integer>>>>() {
              private int count = 0;

              @Override
              public void emitWatermark(Watermark watermark) {
              }

              @Override
              public void emitLatencyMarker(LatencyMarker latencyMarker) {
              }

              @Override
              public void collect(
                  StreamRecord<WindowedValue<KV<Integer, Integer>>> windowedValueStreamRecord) {

                count++;
                if (count >= numElements) {
                  throw new SuccessException();
                }
              }

              @Override
              public void close() {

              }
            });
      } catch (SuccessException e) {

        assertEquals(Math.max(1, numSplits / numTasks), flinkWrapper.getLocalSplitSources().size());

        // success
        return;
      }
      fail("Read terminated without producing expected number of outputs");
    }

    /**
     * Verify that snapshot/restore work as expected. We bring up a source and cancel
     * after seeing a certain number of elements. Then we snapshot that source,
     * bring up a completely new source that we restore from the snapshot and verify
     * that we see all expected elements in the end.
     */
    @Test
    public void testRestore() throws Exception {
      final int numElements = 20;
      final Object checkpointLock = new Object();
      PipelineOptions options = PipelineOptionsFactory.create();

      // this source will emit exactly NUM_ELEMENTS across all parallel readers,
      // afterwards it will stall. We check whether we also receive NUM_ELEMENTS
      // elements later.
      TestCountingSource source = new TestCountingSource(numElements);
      UnboundedSourceWrapper<KV<Integer, Integer>, TestCountingSource.CounterMark> flinkWrapper =
          new UnboundedSourceWrapper<>(options, source, numSplits);

      assertEquals(numSplits, flinkWrapper.getSplitSources().size());

      StreamSource<
          WindowedValue<KV<Integer, Integer>>,
          UnboundedSourceWrapper<
              KV<Integer, Integer>,
              TestCountingSource.CounterMark>> sourceOperator = new StreamSource<>(flinkWrapper);


      AbstractStreamOperatorTestHarness<WindowedValue<KV<Integer, Integer>>> testHarness =
          new AbstractStreamOperatorTestHarness<>(
              sourceOperator,
              numTasks /* parallelism */,
              numTasks /* max parallelism */,
              0 /* subtask index */);

      testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);

      final Set<KV<Integer, Integer>> emittedElements = new HashSet<>();

      boolean readFirstBatchOfElements = false;

      try {
        testHarness.open();
        sourceOperator.run(checkpointLock,
            new Output<StreamRecord<WindowedValue<KV<Integer, Integer>>>>() {
              private int count = 0;

              @Override
              public void emitWatermark(Watermark watermark) {
              }

              @Override
              public void emitLatencyMarker(LatencyMarker latencyMarker) {
              }

              @Override
              public void collect(
                  StreamRecord<WindowedValue<KV<Integer, Integer>>> windowedValueStreamRecord) {

                emittedElements.add(windowedValueStreamRecord.getValue().getValue());
                count++;
                if (count >= 1) {
                  throw new SuccessException();
                }
              }

              @Override
              public void close() {

              }
            });
      } catch (SuccessException e) {
        // success
        readFirstBatchOfElements = true;
      }

      assertTrue("Did not successfully read first batch of elements.", readFirstBatchOfElements);

      // draw a snapshot
      OperatorStateHandles snapshot = testHarness.snapshot(0, 0);

      // test that finalizeCheckpoint on CheckpointMark is called
      final ArrayList<Integer> finalizeList = new ArrayList<>();
      TestCountingSource.setFinalizeTracker(finalizeList);
      testHarness.notifyOfCompletedCheckpoint(0);
      assertEquals(flinkWrapper.getLocalSplitSources().size(), finalizeList.size());

      // create a completely new source but restore from the snapshot
      TestCountingSource restoredSource = new TestCountingSource(numElements);
      UnboundedSourceWrapper<
          KV<Integer, Integer>, TestCountingSource.CounterMark> restoredFlinkWrapper =
          new UnboundedSourceWrapper<>(options, restoredSource, numSplits);

      assertEquals(numSplits, restoredFlinkWrapper.getSplitSources().size());

      StreamSource<
          WindowedValue<KV<Integer, Integer>>,
          UnboundedSourceWrapper<
              KV<Integer, Integer>,
              TestCountingSource.CounterMark>> restoredSourceOperator =
          new StreamSource<>(restoredFlinkWrapper);


      AbstractStreamOperatorTestHarness<WindowedValue<KV<Integer, Integer>>> restoredTestHarness =
          new AbstractStreamOperatorTestHarness<>(
              restoredSourceOperator,
              numTasks /* parallelism */,
              numTasks /* max parallelism */,
              0 /* subtask index */);

      restoredTestHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);

      // restore snapshot
      restoredTestHarness.initializeState(snapshot);

      boolean readSecondBatchOfElements = false;

      // run again and verify that we see the other elements
      try {
        restoredTestHarness.open();
        restoredSourceOperator.run(checkpointLock,
            new Output<StreamRecord<WindowedValue<KV<Integer, Integer>>>>() {
              private int count = 0;

              @Override
              public void emitWatermark(Watermark watermark) {
              }

              @Override
              public void emitLatencyMarker(LatencyMarker latencyMarker) {
              }

              @Override
              public void collect(
                  StreamRecord<WindowedValue<KV<Integer, Integer>>> windowedValueStreamRecord) {
                emittedElements.add(windowedValueStreamRecord.getValue().getValue());
                count++;
                if (count >= numElements / 2) {
                  throw new SuccessException();
                }
              }

              @Override
              public void close() {

              }
            });
      } catch (SuccessException e) {
        // success
        readSecondBatchOfElements = true;
      }

      assertEquals(
          Math.max(1, numSplits / numTasks),
          restoredFlinkWrapper.getLocalSplitSources().size());

      assertTrue("Did not successfully read second batch of elements.", readSecondBatchOfElements);

      // verify that we saw all NUM_ELEMENTS elements
      assertTrue(emittedElements.size() == numElements);
    }

    @Test
    public void testNullCheckpoint() throws Exception {
      final int numElements = 20;
      PipelineOptions options = PipelineOptionsFactory.create();

      TestCountingSource source = new TestCountingSource(numElements) {
        @Override
        public Coder<CounterMark> getCheckpointMarkCoder() {
          return null;
        }
      };

      UnboundedSourceWrapper<KV<Integer, Integer>, TestCountingSource.CounterMark> flinkWrapper =
          new UnboundedSourceWrapper<>(options, source, numSplits);

      StreamSource<
          WindowedValue<KV<Integer, Integer>>,
          UnboundedSourceWrapper<
              KV<Integer, Integer>,
              TestCountingSource.CounterMark>> sourceOperator = new StreamSource<>(flinkWrapper);

      AbstractStreamOperatorTestHarness<WindowedValue<KV<Integer, Integer>>> testHarness =
          new AbstractStreamOperatorTestHarness<>(
              sourceOperator,
              numTasks /* parallelism */,
              numTasks /* max parallelism */,
              0 /* subtask index */);

      testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);

      testHarness.open();

      OperatorStateHandles snapshot = testHarness.snapshot(0, 0);

      UnboundedSourceWrapper<
          KV<Integer, Integer>, TestCountingSource.CounterMark> restoredFlinkWrapper =
          new UnboundedSourceWrapper<>(options, new TestCountingSource(numElements), numSplits);

      StreamSource<
          WindowedValue<KV<Integer, Integer>>,
          UnboundedSourceWrapper<
              KV<Integer, Integer>,
              TestCountingSource.CounterMark>> restoredSourceOperator =
          new StreamSource<>(restoredFlinkWrapper);

      AbstractStreamOperatorTestHarness<WindowedValue<KV<Integer, Integer>>> restoredTestHarness =
          new AbstractStreamOperatorTestHarness<>(
              restoredSourceOperator,
              numTasks /* parallelism */,
              numTasks /* max parallelism */,
              0 /* subtask index */);

      restoredTestHarness.setup();
      restoredTestHarness.initializeState(snapshot);
      restoredTestHarness.open();

      // when the source checkpointed a null we don't re-initialize the splits, that is we
      // will have no splits.
      assertEquals(0, restoredFlinkWrapper.getLocalSplitSources().size());

    }

    /**
     * A special {@link RuntimeException} that we throw to signal that the test was successful.
     */
    private static class SuccessException extends RuntimeException {
    }
  }

  /**
   * Not parameterized tests.
   */
  public static class BasicTest {

    /**
     * Check serialization a {@link UnboundedSourceWrapper}.
     */
    @Test
    public void testSerialization() throws Exception {
      final int parallelism = 1;
      final int numElements = 20;
      PipelineOptions options = PipelineOptionsFactory.create();

      TestCountingSource source = new TestCountingSource(numElements);
      UnboundedSourceWrapper<KV<Integer, Integer>, TestCountingSource.CounterMark> flinkWrapper =
          new UnboundedSourceWrapper<>(options, source, parallelism);

      InstantiationUtil.serializeObject(flinkWrapper);
    }

  }
}
