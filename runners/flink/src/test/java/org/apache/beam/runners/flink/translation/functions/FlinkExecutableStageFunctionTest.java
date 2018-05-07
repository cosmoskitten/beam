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
package org.apache.beam.runners.flink.translation.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Struct;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.runners.flink.DistributedCachePool;
import org.apache.beam.runners.flink.FlinkBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

/** Tests for {@link FlinkExecutableStageFunction}. */
@RunWith(JUnit4.class)
public class FlinkExecutableStageFunctionTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private RuntimeContext runtimeContext;
  @Mock private DistributedCache distributedCache;
  @Mock private Collector<RawUnionValue> collector;
  @Mock private StageBundleFactory stageBundleFactory;
  @Mock private DistributedCachePool cachePool;
  @Mock private StateRequestHandler stateRequestHandler;

  // NOTE: ExecutableStage.fromPayload expects exactly one input, so we provide one here. These unit
  // tests in general ignore the executable stage itself and mock around it.
  private final ExecutableStagePayload stagePayload =
      ExecutableStagePayload.newBuilder()
          .setInput("input")
          .setComponents(
              Components.newBuilder()
                  .putPcollections("input", PCollection.getDefaultInstance())
                  .build())
          .build();
  private final JobInfo jobInfo = JobInfo.create("job-id", "job-name", Struct.getDefaultInstance());

  @Before
  public void setUpMocks() {
    MockitoAnnotations.initMocks(this);
    when(runtimeContext.getDistributedCache()).thenReturn(distributedCache);
  }

  @Test
  public void sdkErrorsSurfaceOnClose() throws Exception {
    FlinkExecutableStageFunction<Integer> function = getFunction(Collections.emptyMap());
    function.open(new Configuration());

    @SuppressWarnings("unchecked")
    RemoteBundle<Integer> bundle = Mockito.mock(RemoteBundle.class);
    when(stageBundleFactory.<Integer>getBundle(any(), any())).thenReturn(bundle);

    @SuppressWarnings("unchecked")
    FnDataReceiver<WindowedValue<Integer>> receiver = Mockito.mock(FnDataReceiver.class);
    when(bundle.getInputReceiver()).thenReturn(receiver);

    Exception expected = new Exception();
    doThrow(expected).when(bundle).close();
    thrown.expect(is(expected));
    function.mapPartition(Collections.emptyList(), collector);
  }

  @Test
  public void checksForRuntimeContextChanges() throws Exception {
    FlinkExecutableStageFunction<Integer> function = getFunction(Collections.emptyMap());
    function.open(new Configuration());
    // Change runtime context.
    function.setRuntimeContext(Mockito.mock(RuntimeContext.class));
    thrown.expect(Matchers.instanceOf(IllegalStateException.class));
    function.mapPartition(Collections.emptyList(), collector);
  }

  @Test
  public void inputsAreSentInOrder() throws Exception {
    FlinkExecutableStageFunction<Integer> function = getFunction(Collections.emptyMap());
    function.open(new Configuration());

    @SuppressWarnings("unchecked")
    RemoteBundle<Integer> bundle = Mockito.mock(RemoteBundle.class);
    when(stageBundleFactory.<Integer>getBundle(any(), any())).thenReturn(bundle);

    @SuppressWarnings("unchecked")
    FnDataReceiver<WindowedValue<Integer>> receiver = Mockito.mock(FnDataReceiver.class);
    when(bundle.getInputReceiver()).thenReturn(receiver);

    WindowedValue<Integer> one = WindowedValue.valueInGlobalWindow(1);
    WindowedValue<Integer> two = WindowedValue.valueInGlobalWindow(2);
    WindowedValue<Integer> three = WindowedValue.valueInGlobalWindow(3);
    function.mapPartition(Arrays.asList(one, two, three), collector);

    InOrder order = Mockito.inOrder(receiver);
    order.verify(receiver).accept(one);
    order.verify(receiver).accept(two);
    order.verify(receiver).accept(three);
  }

  @Test
  public void outputsAreTaggedCorrectly() throws Exception {
    WindowedValue<Integer> three = WindowedValue.valueInGlobalWindow(3);
    WindowedValue<Integer> four = WindowedValue.valueInGlobalWindow(4);
    WindowedValue<Integer> five = WindowedValue.valueInGlobalWindow(5);
    Map<String, Integer> outputTagMap =
        ImmutableMap.of(
            "one", 1,
            "two", 2,
            "three", 3);
    FlinkExecutableStageFunction<Integer> function = getFunction(outputTagMap);
    function.open(new Configuration());

    CompletableFuture<OutputReceiverFactory> receiverFactoryFuture = new CompletableFuture<>();

    @SuppressWarnings("unchecked")
    RemoteBundle<Integer> bundle = Mockito.mock(RemoteBundle.class);
    // Capture the real receiver factory when a bundle is requested.
    when(stageBundleFactory.<Integer>getBundle(any(), any()))
        .thenAnswer(
            (Answer<RemoteBundle<Integer>>)
                invocation -> {
                  Object[] args = invocation.getArguments();
                  receiverFactoryFuture.complete((OutputReceiverFactory) args[0]);
                  return bundle;
                });

    // When the bundle is closed, send output elements to their respective PCollections. The
    // semantic of bundle.close() is to block until all elements have been received.
    doAnswer(
            (Answer<Void>)
                invocation -> {
                  // The bundle can only possibly be closed after it is created by the stage bundle
                  // factory
                  // above.
                  OutputReceiverFactory receiverFactory = receiverFactoryFuture.getNow(null);
                  assertThat(receiverFactory, is(notNullValue()));

                  // Create receivers for the outputs listed in the output tag map.
                  receiverFactory.create("one").accept(three);
                  receiverFactory.create("two").accept(four);
                  receiverFactory.create("three").accept(five);
                  return null;
                })
        .when(bundle)
        .close();

    function.mapPartition(Collections.emptyList(), collector);
    // Ensure that the tagged values sent to the collector have the correct union tags as specified
    // in the output map.
    verify(collector).collect(new RawUnionValue(1, three));
    verify(collector).collect(new RawUnionValue(2, four));
    verify(collector).collect(new RawUnionValue(3, five));
    verifyNoMoreInteractions(collector);
  }

  @Test
  public void testStageBundleClosed() throws Exception {
    FlinkExecutableStageFunction<Integer> function = getFunction(Collections.emptyMap());
    function.open(new Configuration());
    function.close();
    verify(stageBundleFactory).close();
    verifyNoMoreInteractions(stageBundleFactory);
  }

  /**
   * Creates a {@link FlinkExecutableStageFunction}. Intermediate bundle factories are mocked to
   * return the interesting objects, namely {@link #stageBundleFactory}, {@link #cachePool}, and
   * {@link #stateRequestHandler}. These interesting objects are not altered and are expected to be
   * mocked by individual test cases.
   */
  private FlinkExecutableStageFunction<Integer> getFunction(Map<String, Integer> outputMap) {
    JobBundleFactory jobBundleFactory = Mockito.mock(JobBundleFactory.class);
    when(jobBundleFactory.forStage(any())).thenReturn(stageBundleFactory);
    FlinkBundleFactory flinkBundleFactory = Mockito.mock(FlinkBundleFactory.class);
    when(flinkBundleFactory.getJobBundleFactory(any(), any())).thenReturn(jobBundleFactory);

    DistributedCachePool.Factory cachePoolFactory =
        Mockito.mock(DistributedCachePool.Factory.class);
    when(cachePoolFactory.forJob(any())).thenReturn(cachePool);

    FlinkStateRequestHandlerFactory stateHandlerFactory =
        Mockito.mock(FlinkStateRequestHandlerFactory.class);
    when(stateHandlerFactory.forStage(any(), any())).thenReturn(stateRequestHandler);

    FlinkExecutableStageFunction<Integer> function =
        new FlinkExecutableStageFunction<>(
            stagePayload,
            jobInfo,
            outputMap,
            () -> flinkBundleFactory,
            cachePoolFactory,
            stateHandlerFactory);
    function.setRuntimeContext(runtimeContext);
    return function;
  }
}
