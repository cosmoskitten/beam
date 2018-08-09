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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.RunnerPCollectionView;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.flink.translation.functions.FlinkAssignWindows;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageContext;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.utils.FlinkPipelineTranslatorUtils;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.ExecutableStageDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItem;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItemCoder;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WindowDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WorkItemKeySelector;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.protobuf.v3.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Translate an unbounded portable pipeline representation into a Flink pipeline representation. */
public class FlinkStreamingPortablePipelineTranslator
    implements FlinkPortablePipelineTranslator<
        FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext> {

  /**
   * Creates a streaming translation context. The resulting Flink execution dag will live in a new
   * {@link StreamExecutionEnvironment}.
   */
  public static StreamingTranslationContext createTranslationContext(
      JobInfo jobInfo, List<String> filesToStage) {
    PipelineOptions pipelineOptions;
    try {
      pipelineOptions = PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    StreamExecutionEnvironment executionEnvironment =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            pipelineOptions.as(FlinkPipelineOptions.class), filesToStage);
    return new StreamingTranslationContext(jobInfo, pipelineOptions, executionEnvironment);
  }

  /**
   * Streaming translation context. Stores metadata about known PCollections/DataStreams and holds
   * the Flink {@link StreamExecutionEnvironment} that the execution plan will be applied to.
   */
  public static class StreamingTranslationContext
      implements FlinkPortablePipelineTranslator.TranslationContext {

    private final JobInfo jobInfo;
    private final PipelineOptions options;
    private final StreamExecutionEnvironment executionEnvironment;
    private final Map<String, DataStream<?>> dataStreams;

    private StreamingTranslationContext(
        JobInfo jobInfo, PipelineOptions options, StreamExecutionEnvironment executionEnvironment) {
      this.jobInfo = jobInfo;
      this.options = options;
      this.executionEnvironment = executionEnvironment;
      dataStreams = new HashMap<>();
    }

    @Override
    public JobInfo getJobInfo() {
      return jobInfo;
    }

    public PipelineOptions getPipelineOptions() {
      return options;
    }

    public StreamExecutionEnvironment getExecutionEnvironment() {
      return executionEnvironment;
    }

    public <T> void addDataStream(String pCollectionId, DataStream<T> dataSet) {
      dataStreams.put(pCollectionId, dataSet);
    }

    public <T> DataStream<T> getDataStreamOrThrow(String pCollectionId) {
      DataStream<T> dataSet = (DataStream<T>) dataStreams.get(pCollectionId);
      if (dataSet == null) {
        throw new IllegalArgumentException(
            String.format("Unknown datastream for id %s.", pCollectionId));
      }
      return dataSet;
    }
  }

  interface PTransformTranslator<T> {
    void translate(String id, RunnerApi.Pipeline pipeline, T t);
  }

  private final Map<String, PTransformTranslator<StreamingTranslationContext>>
      urnToTransformTranslator;

  FlinkStreamingPortablePipelineTranslator() {
    ImmutableMap.Builder<String, PTransformTranslator<StreamingTranslationContext>> translatorMap =
        ImmutableMap.builder();
    translatorMap.put(PTransformTranslation.FLATTEN_TRANSFORM_URN, this::translateFlatten);
    translatorMap.put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, this::translateGroupByKey);
    translatorMap.put(PTransformTranslation.IMPULSE_TRANSFORM_URN, this::translateImpulse);
    translatorMap.put(
        PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, this::translateAssignWindows);
    translatorMap.put(ExecutableStage.URN, this::translateExecutableStage);
    translatorMap.put(PTransformTranslation.RESHUFFLE_URN, this::translateReshuffle);
    // TODO: is this needed
    translatorMap.put(
            PTransformTranslation.CREATE_VIEW_TRANSFORM_URN,
            this::translateView);

    this.urnToTransformTranslator = translatorMap.build();
  }

  @Override
  public void translate(StreamingTranslationContext context, RunnerApi.Pipeline pipeline) {
    QueryablePipeline p =
        QueryablePipeline.forTransforms(
            pipeline.getRootTransformIdsList(), pipeline.getComponents());
    for (PipelineNode.PTransformNode transform : p.getTopologicallyOrderedTransforms()) {
      urnToTransformTranslator
          .getOrDefault(transform.getTransform().getSpec().getUrn(), this::urnNotFound)
          .translate(transform.getId(), pipeline, context);
    }
  }

  private void urnNotFound(
      String id,
      RunnerApi.Pipeline pipeline,
      FlinkStreamingPortablePipelineTranslator.TranslationContext context) {
    throw new IllegalArgumentException(
        String.format(
            "Unknown type of URN %s for PTrasnform with id %s.",
            pipeline.getComponents().getTransformsOrThrow(id).getSpec().getUrn(), id));
  }

  private <InputT> void translateView(
          String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {

    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(id);
    DataStream<WindowedValue<InputT>> inputDataStream =
            context.getDataStreamOrThrow(
                    Iterables.getOnlyElement(transform.getInputsMap().values()));

    context.addDataStream(
            Iterables.getOnlyElement(transform.getOutputsMap().values()), inputDataStream);
  }

  private <K, V> void translateReshuffle(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {
    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(id);
    DataStream<WindowedValue<KV<K, V>>> inputDataStream =
        context.getDataStreamOrThrow(Iterables.getOnlyElement(transform.getInputsMap().values()));
    context.addDataStream(
        Iterables.getOnlyElement(transform.getOutputsMap().values()), inputDataStream.rebalance());
  }

  private <T> void translateFlatten(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {
    Map<String, String> allInputs =
        pipeline.getComponents().getTransformsOrThrow(id).getInputsMap();

    if (allInputs.isEmpty()) {

      // create an empty dummy source to satisfy downstream operations
      // we cannot create an empty source in Flink, therefore we have to
      // add the flatMap that simply never forwards the single element
      DataStreamSource<String> dummySource =
          context.getExecutionEnvironment().fromElements("dummy");

      DataStream<WindowedValue<T>> result =
          dummySource
              .<WindowedValue<T>>flatMap(
                  (s, collector) -> {
                    // never return anything
                  })
              .returns(
                  new CoderTypeInformation<>(
                      WindowedValue.getFullCoder(
                          (Coder<T>) VoidCoder.of(), GlobalWindow.Coder.INSTANCE)));
      context.addDataStream(
          Iterables.getOnlyElement(
              pipeline.getComponents().getTransformsOrThrow(id).getOutputsMap().values()),
          result);
    } else {
      DataStream<T> result = null;

      // Determine DataStreams that we use as input several times. For those, we need to uniquify
      // input streams because Flink seems to swallow watermarks when we have a union of one and
      // the same stream.
      HashMultiset<DataStream<T>> inputCounts = HashMultiset.create();
      for (String input : allInputs.values()) {
        DataStream<T> current = context.getDataStreamOrThrow(input);
        inputCounts.add(current, 1);
      }

      for (String input : allInputs.values()) {
        DataStream<T> current = context.getDataStreamOrThrow(input);
        final int timesRequired = inputCounts.count(current);
        if (timesRequired > 1) {
          current =
              current.flatMap(
                  new FlatMapFunction<T, T>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void flatMap(T t, Collector<T> collector) {
                      collector.collect(t);
                    }
                  });
        }
        result = (result == null) ? current : result.union(current);
      }

      context.addDataStream(
          Iterables.getOnlyElement(
              pipeline.getComponents().getTransformsOrThrow(id).getOutputsMap().values()),
          result);
    }
  }

  private <K, V> void translateGroupByKey(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {

    RunnerApi.PTransform pTransform = pipeline.getComponents().getTransformsOrThrow(id);
    String inputPCollectionId = Iterables.getOnlyElement(pTransform.getInputsMap().values());

    RunnerApi.WindowingStrategy windowingStrategyProto =
        pipeline
            .getComponents()
            .getWindowingStrategiesOrThrow(
                pipeline
                    .getComponents()
                    .getPcollectionsOrThrow(inputPCollectionId)
                    .getWindowingStrategyId());

    DataStream<WindowedValue<KV<K, V>>> inputDataStream =
        context.getDataStreamOrThrow(inputPCollectionId);

    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(pipeline.getComponents());

    WindowingStrategy<?, ?> windowingStrategy;
    try {
      windowingStrategy =
          WindowingStrategyTranslation.fromProto(windowingStrategyProto, rehydratedComponents);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          String.format(
              "Unable to hydrate GroupByKey windowing strategy %s.", windowingStrategyProto),
          e);
    }

    WindowedValueCoder<KV<K, V>> windowedInputCoder =
        (WindowedValueCoder) instantiateCoder(inputPCollectionId, pipeline.getComponents());
    KvCoder<K, V> inputElementCoder = (KvCoder<K, V>) windowedInputCoder.getValueCoder();

    SingletonKeyedWorkItemCoder<K, V> workItemCoder =
        SingletonKeyedWorkItemCoder.of(
            inputElementCoder.getKeyCoder(),
            inputElementCoder.getValueCoder(),
            windowingStrategy.getWindowFn().windowCoder());

    WindowedValue.FullWindowedValueCoder<SingletonKeyedWorkItem<K, V>> windowedWorkItemCoder =
        WindowedValue.getFullCoder(workItemCoder, windowingStrategy.getWindowFn().windowCoder());

    CoderTypeInformation<WindowedValue<SingletonKeyedWorkItem<K, V>>> workItemTypeInfo =
        new CoderTypeInformation<>(windowedWorkItemCoder);

    DataStream<WindowedValue<SingletonKeyedWorkItem<K, V>>> workItemStream =
        inputDataStream
            .flatMap(new FlinkStreamingTransformTranslators.ToKeyedWorkItem<>())
            .returns(workItemTypeInfo)
            .name("ToKeyedWorkItem");

    WorkItemKeySelector<K, V> keySelector =
        new WorkItemKeySelector<>(inputElementCoder.getKeyCoder());

    KeyedStream<WindowedValue<SingletonKeyedWorkItem<K, V>>, ByteBuffer> keyedWorkItemStream =
        workItemStream.keyBy(keySelector);

    SystemReduceFn<K, V, Iterable<V>, Iterable<V>, BoundedWindow> reduceFn =
        SystemReduceFn.buffering(inputElementCoder.getValueCoder());

    Coder<Iterable<V>> accumulatorCoder = IterableCoder.of(inputElementCoder.getValueCoder());

    Coder<WindowedValue<KV<K, Iterable<V>>>> outputCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(inputElementCoder.getKeyCoder(), accumulatorCoder),
            windowingStrategy.getWindowFn().windowCoder());

    TypeInformation<WindowedValue<KV<K, Iterable<V>>>> outputTypeInfo =
        new CoderTypeInformation<>(outputCoder);

    TupleTag<KV<K, Iterable<V>>> mainTag = new TupleTag<>("main output");

    // TODO: remove non-portable operator re-use
    WindowDoFnOperator<K, V, Iterable<V>> doFnOperator =
        new WindowDoFnOperator<K, V, Iterable<V>>(
            reduceFn,
            pTransform.getUniqueName(),
            (Coder) windowedWorkItemCoder,
            mainTag,
            Collections.emptyList(),
            new DoFnOperator.MultiOutputOutputManagerFactory(mainTag, outputCoder),
            windowingStrategy,
            new HashMap<>(), /* side-input mapping */
            Collections.emptyList(), /* side inputs */
            context.getPipelineOptions(),
            inputElementCoder.getKeyCoder(),
            (KeySelector) keySelector /* key selector */);

    SingleOutputStreamOperator<WindowedValue<KV<K, Iterable<V>>>> outputDataStream =
        keyedWorkItemStream.transform(
            pTransform.getUniqueName(), outputTypeInfo, (OneInputStreamOperator) doFnOperator);

    context.addDataStream(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()), outputDataStream);
  }

  private void translateImpulse(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {
    RunnerApi.PTransform pTransform = pipeline.getComponents().getTransformsOrThrow(id);

    TypeInformation<WindowedValue<byte[]>> typeInfo =
        new CoderTypeInformation<>(
            WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE));

    DataStreamSource<WindowedValue<byte[]>> source =
        context
            .getExecutionEnvironment()
            .fromCollection(
                Collections.singleton(WindowedValue.valueInGlobalWindow(new byte[0])), typeInfo);

    context.addDataStream(Iterables.getOnlyElement(pTransform.getOutputsMap().values()), source);
  }

  private <T> void translateAssignWindows(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {
    RunnerApi.Components components = pipeline.getComponents();
    RunnerApi.PTransform transform = components.getTransformsOrThrow(id);
    RunnerApi.WindowIntoPayload payload;
    try {
      payload = RunnerApi.WindowIntoPayload.parseFrom(transform.getSpec().getPayload());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(e);
    }
    //TODO: https://issues.apache.org/jira/browse/BEAM-4296
    // This only works for well known window fns, we should defer this execution to the SDK
    // if the WindowFn can't be parsed or just defer it all the time.
    WindowFn<T, ? extends BoundedWindow> windowFn =
        (WindowFn<T, ? extends BoundedWindow>)
            WindowingStrategyTranslation.windowFnFromProto(payload.getWindowFn());

    String inputCollectionId = Iterables.getOnlyElement(transform.getInputsMap().values());
    String outputCollectionId = Iterables.getOnlyElement(transform.getOutputsMap().values());
    Coder<WindowedValue<T>> outputCoder = instantiateCoder(outputCollectionId, components);
    TypeInformation<WindowedValue<T>> resultTypeInfo = new CoderTypeInformation<>(outputCoder);

    DataStream<WindowedValue<T>> inputDataStream = context.getDataStreamOrThrow(inputCollectionId);

    FlinkAssignWindows<T, ? extends BoundedWindow> assignWindowsFunction =
        new FlinkAssignWindows<>(windowFn);

    DataStream<WindowedValue<T>> resultDataStream =
        inputDataStream
            .flatMap(assignWindowsFunction)
            .name(transform.getUniqueName())
            .returns(resultTypeInfo);

    context.addDataStream(outputCollectionId, resultDataStream);
  }

  private <InputT, OutputT> void translateExecutableStage(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {
    // TODO: Fail on stateful DoFns for now.
    // TODO: Support stateful DoFns by inserting group-by-keys where necessary.
    // TODO: Fail on splittable DoFns.
    // TODO: Special-case single outputs to avoid multiplexing PCollections.
    RunnerApi.Components components = pipeline.getComponents();
    RunnerApi.PTransform transform = components.getTransformsOrThrow(id);
    Map<String, String> outputs = transform.getOutputsMap();

    final RunnerApi.ExecutableStagePayload stagePayload;
    try {
      stagePayload = RunnerApi.ExecutableStagePayload.parseFrom(transform.getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    String inputPCollectionId = stagePayload.getInput();
    final Tuple2<Map<Integer, PCollectionView<?>>, DataStream<RawUnionValue>> transformedSideInputs;

    // TODO: https://issues.apache.org/jira/browse/BEAM-2930
    if (stagePayload.getSideInputsCount() > 0) {
      //throw new UnsupportedOperationException(
      //    "[BEAM-2930] streaming translator does not support side inputs: " + transform);
      transformedSideInputs = transformSideInputs(stagePayload, components, context);
    } else {
      transformedSideInputs = new Tuple2<>(Collections.emptyMap(), null);
    }

    Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags = Maps.newLinkedHashMap();
    Map<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders = Maps.newLinkedHashMap();
    // TODO: does it matter which output we designate as "main"
    final TupleTag<OutputT> mainOutputTag =
        outputs.isEmpty() ? null : new TupleTag(outputs.keySet().iterator().next());

    // associate output tags with ids, output manager uses these Integer ids to serialize state
    BiMap<String, Integer> outputIndexMap =
        FlinkPipelineTranslatorUtils.createOutputMap(outputs.keySet());
    Map<String, Coder<WindowedValue<?>>> outputCoders = Maps.newHashMap();
    Map<TupleTag<?>, Integer> tagsToIds = Maps.newHashMap();
    Map<String, TupleTag<?>> collectionIdToTupleTag = Maps.newHashMap();
    // order output names for deterministic mapping
    for (String localOutputName : new TreeMap<>(outputIndexMap).keySet()) {
      String collectionId = outputs.get(localOutputName);
      Coder<WindowedValue<?>> windowCoder = (Coder) instantiateCoder(collectionId, components);
      outputCoders.put(localOutputName, windowCoder);
      TupleTag<?> tupleTag = new TupleTag<>(localOutputName);
      CoderTypeInformation<WindowedValue<?>> typeInformation =
          new CoderTypeInformation(windowCoder);
      tagsToOutputTags.put(tupleTag, new OutputTag<>(localOutputName, typeInformation));
      tagsToCoders.put(tupleTag, windowCoder);
      tagsToIds.put(tupleTag, outputIndexMap.get(localOutputName));
      collectionIdToTupleTag.put(collectionId, tupleTag);
    }

    final SingleOutputStreamOperator<WindowedValue<OutputT>> outputStream;
    DataStream<WindowedValue<InputT>> inputDataStream =
        context.getDataStreamOrThrow(inputPCollectionId);

    // TODO: coder for side input push back
    final Coder<WindowedValue<InputT>> windowedInputCoder = null;
    CoderTypeInformation<WindowedValue<OutputT>> outputTypeInformation =
        (!outputs.isEmpty())
            ? new CoderTypeInformation(outputCoders.get(mainOutputTag.getId()))
            : null;

    ArrayList<TupleTag<?>> additionalOutputTags = Lists.newArrayList();
    for (TupleTag<?> tupleTag : tagsToCoders.keySet()) {
      if (!mainOutputTag.getId().equals(tupleTag.getId())) {
        additionalOutputTags.add(tupleTag);
      }
    }

    DoFnOperator.MultiOutputOutputManagerFactory<OutputT> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory<>(
            mainOutputTag, tagsToOutputTags, tagsToCoders, tagsToIds);

    DoFnOperator<InputT, OutputT> doFnOperator =
        new ExecutableStageDoFnOperator<>(
            transform.getUniqueName(),
            windowedInputCoder,
            null,
            Collections.emptyMap(),
            mainOutputTag,
            additionalOutputTags,
            outputManagerFactory,
            transformedSideInputs.f0,
            new ArrayList<>(transformedSideInputs.f0.values()),
            context.getPipelineOptions(),
            stagePayload,
            context.getJobInfo(),
            FlinkExecutableStageContext.batchFactory(),
            collectionIdToTupleTag);

    if (transformedSideInputs.f0.isEmpty()) {
      outputStream =
              inputDataStream.transform(transform.getUniqueName(), outputTypeInformation, doFnOperator);
    } else {
      outputStream =
              inputDataStream
                      .connect(transformedSideInputs.f1.broadcast())
                      .transform(transform.getUniqueName(), outputTypeInformation, doFnOperator);
    }

    if (mainOutputTag != null) {
      context.addDataStream(outputs.get(mainOutputTag.getId()), outputStream);
    }

    for (TupleTag<?> tupleTag : additionalOutputTags) {
      context.addDataStream(
          outputs.get(tupleTag.getId()),
          outputStream.getSideOutput(tagsToOutputTags.get(tupleTag)));
    }
  }

  private static Tuple2<Map<Integer, PCollectionView<?>>, DataStream<RawUnionValue>>
  transformSideInputs(RunnerApi.ExecutableStagePayload stagePayload,
                      RunnerApi.Components components,
          StreamingTranslationContext context) {

    RehydratedComponents rehydratedComponents =
            RehydratedComponents.forComponents(components);

    LinkedHashMap<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInputs = new LinkedHashMap<>();
    for (RunnerApi.ExecutableStagePayload.SideInputId sideInputId : stagePayload.getSideInputsList()) {

      // following is required to rehydrate the PCollectionView from the proto,
      // since the Flink operator depends on it

      // the transform this side input belongs to
      //try {
        //RunnerApi.PTransform parDoProto = components.getTransformsOrThrow(sideInputId.getTransformId());
        //RunnerApi.ParDoPayload payload = RunnerApi.ParDoPayload.parseFrom(parDoProto.getSpec().getPayload());
      //} catch (IOException ex) {
      //  throw new RuntimeException("Failed to extract side input view information.", ex);
      //}

      String sideInputTag = sideInputId.getLocalName();
      ViewFn<Iterable<WindowedValue<?>>, ?> viewFn = (ViewFn) new PCollectionViews.MultimapViewFn<Iterable<WindowedValue<Void>>, Void>();

      String collectionId = components.getTransformsOrThrow(sideInputId.getTransformId())
                      .getInputsOrThrow(sideInputId.getLocalName());
      RunnerApi.WindowingStrategy windowingStrategyProto = components.getWindowingStrategiesOrThrow(
              components.getPcollectionsOrThrow(collectionId).getWindowingStrategyId());

      final WindowingStrategy<?, ?> windowingStrategy;
      try {
        windowingStrategy =
                WindowingStrategyTranslation.fromProto(windowingStrategyProto, rehydratedComponents);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException(
                String.format(
                        "Unable to hydrate side input windowing strategy %s.", windowingStrategyProto),
                e);
      }

      // TODO: get coder from stream instead?
      Coder<WindowedValue<Object>> coder = instantiateCoder(collectionId, components);

      sideInputs.put(sideInputId,
              new RunnerPCollectionView<>(
                      null,
                      new TupleTag<>(sideInputTag),
                      viewFn,
                      // TODO: support custom mapping fn
                      windowingStrategy.getWindowFn().getDefaultWindowMappingFn(),
                      windowingStrategy,
                      coder)
      );

    }

  // from FlinkStreamingTransformTranslators

    // TODO: combine with previous loop
    // collect all side inputs
    Map<TupleTag<?>, Integer> tagToIntMapping = new HashMap<>();
    Map<Integer, PCollectionView<?>> intToViewMapping = new HashMap<>();
    List<Coder<?>> inputCoders = new ArrayList<>();

    int count = 0;
    for (Map.Entry<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInput : sideInputs.entrySet()) {
      TupleTag<?> tag = sideInput.getValue().getTagInternal();
      intToViewMapping.put(count, sideInput.getValue());
      tagToIntMapping.put(tag, count);
      count++;
      String collectionId = components.getTransformsOrThrow(sideInput.getKey().getTransformId())
                      .getInputsOrThrow(sideInput.getKey().getLocalName());
      DataStream<Object> sideInputStream = context.getDataStreamOrThrow(collectionId);
      TypeInformation<Object> tpe = sideInputStream.getType();
      if (!(tpe instanceof CoderTypeInformation)) {
        throw new IllegalStateException("Input Stream TypeInformation is no CoderTypeInformation.");
      }

      // wrap for iterable map below
      WindowedValueCoder coder = (WindowedValueCoder)((CoderTypeInformation) tpe).getCoder();
      coder = coder.withValueCoder(IterableCoder.of(coder.getValueCoder()));
      inputCoders.add(coder);
    }

    // second pass, now that we collected the input coders
    UnionCoder unionCoder = UnionCoder.of(inputCoders);

    CoderTypeInformation<RawUnionValue> unionTypeInformation =
            new CoderTypeInformation<>(unionCoder);

    // transform each side input to RawUnionValue and union them
    DataStream<RawUnionValue> sideInputUnion = null;

    for (Map.Entry<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInput : sideInputs.entrySet()) {
      TupleTag<?> tag = sideInput.getValue().getTagInternal();
      final int intTag = tagToIntMapping.get(tag);
      String collectionId = components.getTransformsOrThrow(sideInput.getKey().getTransformId())
                      .getInputsOrThrow(sideInput.getKey().getLocalName());
      DataStream<WindowedValue<?>> sideInputStream = context.getDataStreamOrThrow(collectionId);
      // the side input handler expects an iterable..
      DataStream<WindowedValue<Iterable<?>>> iterableStream = sideInputStream.map(new WrapAsIterable());
      DataStream<RawUnionValue> unionValueStream = iterableStream.map(
              new FlinkStreamingTransformTranslators.ToRawUnion<>(intTag)).returns(unionTypeInformation);

      if (sideInputUnion == null) {
        sideInputUnion = unionValueStream;
      } else {
        sideInputUnion = sideInputUnion.union(unionValueStream);
      }
    }

    return new Tuple2<>(intToViewMapping, sideInputUnion);
  }

  private static class WrapAsIterable implements MapFunction<WindowedValue<?>, WindowedValue<Iterable<?>>> {
    @Override
    public WindowedValue<Iterable<?>> map(WindowedValue<?> value) {
      return value.withValue(Collections.singletonList(value.getValue()));
    }
  }

  static <T> Coder<WindowedValue<T>> instantiateCoder(
      String collectionId, RunnerApi.Components components) {
    PipelineNode.PCollectionNode collectionNode =
        PipelineNode.pCollection(collectionId, components.getPcollectionsOrThrow(collectionId));
    try {
      return WireCoders.instantiateRunnerWireCoder(collectionNode, components);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
