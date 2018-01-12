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

import static org.apache.beam.runners.core.construction.SplittableParDo.SPLITTABLE_PROCESS_URN;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.CombineTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.runners.flink.translation.functions.FlinkAssignWindows;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.KvToByteBufferKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItem;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SingletonKeyedWorkItemCoder;
import org.apache.beam.runners.flink.translation.wrappers.streaming.SplittableDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WindowDoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.WorkItemKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.BoundedSourceWrapper;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.DedupingOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedSourceWrapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * This class contains all the mappings between Beam and Flink
 * <b>streaming</b> transformations. The {@link FlinkStreamingPipelineTranslator}
 * traverses the Beam job and comes here to translate the encountered Beam transformations
 * into Flink one, based on the mapping available in this class.
 */
class FlinkStreamingTransformTranslators {

  // --------------------------------------------------------------------------------------------
  //  Transform Translator Registry
  // --------------------------------------------------------------------------------------------

  /**
   * A map from a Transform URN to the translator.
   */
  @SuppressWarnings("rawtypes")
  private static final Map<String, FlinkStreamingPipelineTranslator.StreamTransformTranslator>
      TRANSLATORS = new HashMap<>();

  // here you can find all the available translators.
  static {
    TRANSLATORS.put(PTransformTranslation.READ_TRANSFORM_URN, new ReadSourceTranslator());

    TRANSLATORS.put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoStreamingTranslator());
    TRANSLATORS.put(
        SPLITTABLE_PROCESS_URN, new SplittableProcessElementsStreamingTranslator());
    TRANSLATORS.put(SplittableParDo.SPLITTABLE_GBKIKWI_URN, new GBKIntoKeyedWorkItemsTranslator());

    TRANSLATORS.put(PTransformTranslation.WINDOW_TRANSFORM_URN, new WindowAssignTranslator());
    TRANSLATORS.put(
        PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenPCollectionTranslator());
    TRANSLATORS.put(
        CreateStreamingFlinkView.CREATE_STREAMING_FLINK_VIEW_URN,
        new CreateViewStreamingTranslator());

    TRANSLATORS.put(PTransformTranslation.RESHUFFLE_URN, new ReshuffleTranslatorStreaming());
    TRANSLATORS.put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslator());
    TRANSLATORS.put(PTransformTranslation.COMBINE_TRANSFORM_URN, new CombinePerKeyTranslator());
  }

  public static FlinkStreamingPipelineTranslator.StreamTransformTranslator<?> getTranslator(
      PTransform<?, ?> transform) {
    @Nullable String urn = PTransformTranslation.urnForTransformOrNull(transform);
    return urn == null ? null : TRANSLATORS.get(urn);
  }

  // --------------------------------------------------------------------------------------------
  //  Transformation Implementations
  // --------------------------------------------------------------------------------------------

  private static class UnboundedReadSourceTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
      PTransform<PBegin, PCollection<T>>> {

    @Override
    public void translateNode(
        PTransform<PBegin, PCollection<T>> transform,
        FlinkStreamingTranslationContext context) {
      PCollection<T> output = context.getOutput(transform);

      DataStream<WindowedValue<T>> source;
      DataStream<WindowedValue<ValueWithRecordId<T>>> nonDedupSource;
      TypeInformation<WindowedValue<T>> outputTypeInfo =
          context.getTypeInfo(context.getOutput(transform));

      Coder<T> coder = context.getOutput(transform).getCoder();

      TypeInformation<WindowedValue<ValueWithRecordId<T>>> withIdTypeInfo =
          new CoderTypeInformation<>(WindowedValue.getFullCoder(
              ValueWithRecordId.ValueWithRecordIdCoder.of(coder),
              output.getWindowingStrategy().getWindowFn().windowCoder()));

      UnboundedSource<T, ?> rawSource;
      try {
        rawSource = ReadTranslation.unboundedSourceFromTransform(
            (AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>>)
                context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      try {

        UnboundedSourceWrapper<T, ?> sourceWrapper =
            new UnboundedSourceWrapper<>(
                context.getCurrentTransform().getFullName(),
                context.getPipelineOptions(),
                rawSource,
                context.getExecutionEnvironment().getParallelism());
        nonDedupSource = context
            .getExecutionEnvironment()
            .addSource(sourceWrapper).name(transform.getName()).returns(withIdTypeInfo);

        if (rawSource.requiresDeduping()) {
          source = nonDedupSource.keyBy(
              new ValueWithRecordIdKeySelector<T>())
              .transform("deduping", outputTypeInfo, new DedupingOperator<T>());
        } else {
          source = nonDedupSource.flatMap(new StripIdsMap<T>()).returns(outputTypeInfo);
        }
      } catch (Exception e) {
        throw new RuntimeException(
            "Error while translating UnboundedSource: " + rawSource, e);
      }

      context.setOutputDataStream(output, source);
    }
  }

  private static class ValueWithRecordIdKeySelector<T>
      implements KeySelector<WindowedValue<ValueWithRecordId<T>>, ByteBuffer>,
      ResultTypeQueryable<ByteBuffer> {

    @Override
    public ByteBuffer getKey(WindowedValue<ValueWithRecordId<T>> value) throws Exception {
      return ByteBuffer.wrap(value.getValue().getId());
    }

    @Override
    public TypeInformation<ByteBuffer> getProducedType() {
      return new GenericTypeInfo<>(ByteBuffer.class);
    }
  }

  public static class StripIdsMap<T> implements
      FlatMapFunction<WindowedValue<ValueWithRecordId<T>>, WindowedValue<T>> {

    @Override
    public void flatMap(WindowedValue<ValueWithRecordId<T>> value,
                        Collector<WindowedValue<T>> collector) throws Exception {
      collector.collect(value.withValue(value.getValue().getValue()));
    }

  }

  private static class ReadSourceTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PBegin, PCollection<T>>> {

    private final BoundedReadSourceTranslator<T> boundedTranslator =
        new BoundedReadSourceTranslator<>();
    private final UnboundedReadSourceTranslator<T> unboundedTranslator =
        new UnboundedReadSourceTranslator<>();

    @Override
    void translateNode(
        PTransform<PBegin, PCollection<T>> transform, FlinkStreamingTranslationContext context) {
      if (context.getOutput(transform).isBounded().equals(PCollection.IsBounded.BOUNDED)) {
        boundedTranslator.translateNode(transform, context);
      } else {
        unboundedTranslator.translateNode((Read.Unbounded<T>) transform, context);
      }
    }
  }

  private static class BoundedReadSourceTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PBegin, PCollection<T>>> {

    @Override
    public void translateNode(
        PTransform<PBegin, PCollection<T>> transform,
        FlinkStreamingTranslationContext context) {
      PCollection<T> output = context.getOutput(transform);

      TypeInformation<WindowedValue<T>> outputTypeInfo =
          context.getTypeInfo(context.getOutput(transform));


      BoundedSource<T> rawSource;
      try {
        rawSource = ReadTranslation.boundedSourceFromTransform(
            (AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>>)
                context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      DataStream<WindowedValue<T>> source;
      try {
        BoundedSourceWrapper<T> sourceWrapper =
            new BoundedSourceWrapper<>(
                context.getCurrentTransform().getFullName(),
                context.getPipelineOptions(),
                rawSource,
                context.getExecutionEnvironment().getParallelism());
        source = context
            .getExecutionEnvironment()
            .addSource(sourceWrapper).name(transform.getName()).returns(outputTypeInfo);
      } catch (Exception e) {
        throw new RuntimeException(
            "Error while translating BoundedSource: " + rawSource, e);
      }

      // Add operator directly to source to ensure that the read happens.
      context.getExecutionEnvironment().addOperator(source.getTransformation());
      context.setOutputDataStream(output, source);
    }
  }

  /**
   * Wraps each element in a {@link RawUnionValue} with the given tag id.
   */
  private static class ToRawUnion<T> implements MapFunction<T, RawUnionValue> {
    private final int intTag;

    public ToRawUnion(int intTag) {
      this.intTag = intTag;
    }

    @Override
    public RawUnionValue map(T o) throws Exception {
      return new RawUnionValue(intTag, o);
    }
  }

  private static Tuple2<Map<Integer, PCollectionView<?>>, DataStream<RawUnionValue>>
        transformSideInputs(
          Collection<PCollectionView<?>> sideInputs,
          FlinkStreamingTranslationContext context) {

    // collect all side inputs
    Map<TupleTag<?>, Integer> tagToIntMapping = new HashMap<>();
    Map<Integer, PCollectionView<?>> intToViewMapping = new HashMap<>();
    int count = 0;
    for (PCollectionView<?> sideInput: sideInputs) {
      TupleTag<?> tag = sideInput.getTagInternal();
      intToViewMapping.put(count, sideInput);
      tagToIntMapping.put(tag, count);
      count++;
    }


    List<Coder<?>> inputCoders = new ArrayList<>();
    for (PCollectionView<?> sideInput: sideInputs) {
      DataStream<Object> sideInputStream = context.getInputDataStream(sideInput);
      TypeInformation<Object> tpe = sideInputStream.getType();
      if (!(tpe instanceof CoderTypeInformation)) {
        throw new IllegalStateException(
            "Input Stream TypeInformation is no CoderTypeInformation.");
      }

      Coder<?> coder = ((CoderTypeInformation) tpe).getCoder();
      inputCoders.add(coder);
    }

    UnionCoder unionCoder = UnionCoder.of(inputCoders);

    CoderTypeInformation<RawUnionValue> unionTypeInformation =
        new CoderTypeInformation<>(unionCoder);

    // transform each side input to RawUnionValue and union them
    DataStream<RawUnionValue> sideInputUnion = null;

    for (PCollectionView<?> sideInput: sideInputs) {
      TupleTag<?> tag = sideInput.getTagInternal();
      final int intTag = tagToIntMapping.get(tag);
      DataStream<Object> sideInputStream = context.getInputDataStream(sideInput);
      DataStream<RawUnionValue> unionValueStream =
          sideInputStream.map(new ToRawUnion<>(intTag)).returns(unionTypeInformation);

      if (sideInputUnion == null) {
        sideInputUnion = unionValueStream;
      } else {
        sideInputUnion = sideInputUnion.union(unionValueStream);
      }
    }

    if (sideInputUnion == null) {
      throw new IllegalStateException("No unioned side inputs, this indicates a bug.");
    }

    return new Tuple2<>(intToViewMapping, sideInputUnion);
  }

  /**
   * Helper for translating {@code ParDo.MultiOutput} and {@link
   * SplittableParDoViaKeyedWorkItems.ProcessElements}.
   */
  static class ParDoTranslationHelper {

    interface DoFnOperatorFactory<InputT, OutputT> {
      DoFnOperator<InputT, OutputT> createDoFnOperator(
          DoFn<InputT, OutputT> doFn,
          String stepName,
          List<PCollectionView<?>> sideInputs,
          TupleTag<OutputT> mainOutputTag,
          List<TupleTag<?>> additionalOutputTags,
          FlinkStreamingTranslationContext context,
          WindowingStrategy<?, ?> windowingStrategy,
          Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags,
          Map<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders,
          Map<TupleTag<?>, Integer> tagsToIds,
          Coder<WindowedValue<InputT>> inputCoder,
          Coder keyCoder,
          Map<Integer, PCollectionView<?>> transformedSideInputs);
    }

    static <InputT, OutputT> void translateParDo(
        String transformName,
        DoFn<InputT, OutputT> doFn,
        PCollection<InputT> input,
        List<PCollectionView<?>> sideInputs,
        Map<TupleTag<?>, PValue> outputs,
        TupleTag<OutputT> mainOutputTag,
        List<TupleTag<?>> additionalOutputTags,
        FlinkStreamingTranslationContext context,
        DoFnOperatorFactory<InputT, OutputT> doFnOperatorFactory) {

      // we assume that the transformation does not change the windowing strategy.
      WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();

      Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags = Maps.newHashMap();
      Map<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders = Maps.newHashMap();

      // We associate output tags with ids, the Integer is easier to serialize than TupleTag.
      // The return map of AppliedPTransform.getOutputs() is an ImmutableMap, its implementation is
      // RegularImmutableMap, its entrySet order is the same with the order of insertion.
      // So we can use the original AppliedPTransform.getOutputs() to produce deterministic ids.
      Map<TupleTag<?>, Integer> tagsToIds = Maps.newHashMap();
      int idCount = 0;
      tagsToIds.put(mainOutputTag, idCount++);
      for (Map.Entry<TupleTag<?>, PValue> entry : outputs.entrySet()) {
        if (!tagsToOutputTags.containsKey(entry.getKey())) {
          tagsToOutputTags.put(
              entry.getKey(),
              new OutputTag<WindowedValue<?>>(
                  entry.getKey().getId(),
                  (TypeInformation) context.getTypeInfo((PCollection<?>) entry.getValue())
              )
          );
          tagsToCoders.put(entry.getKey(),
              (Coder) context.getCoder((PCollection<OutputT>) entry.getValue()));
          tagsToIds.put(entry.getKey(), idCount++);
        }
      }

      SingleOutputStreamOperator<WindowedValue<OutputT>> outputStream;

      Coder<WindowedValue<InputT>> inputCoder = context.getCoder(input);

      DataStream<WindowedValue<InputT>> inputDataStream = context.getInputDataStream(input);

      Coder keyCoder = null;
      boolean stateful = false;
      DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
      if (signature.stateDeclarations().size() > 0
          || signature.timerDeclarations().size() > 0) {
        // Based on the fact that the signature is stateful, DoFnSignatures ensures
        // that it is also keyed
        keyCoder = ((KvCoder) input.getCoder()).getKeyCoder();
        inputDataStream = inputDataStream.keyBy(new KvToByteBufferKeySelector(keyCoder));
        stateful = true;
      } else if (doFn instanceof SplittableParDoViaKeyedWorkItems.ProcessFn) {
        // we know that it is keyed on String
        keyCoder = StringUtf8Coder.of();
        stateful = true;
      }

      CoderTypeInformation<WindowedValue<OutputT>> outputTypeInformation =
          new CoderTypeInformation<>(
              context.getCoder((PCollection<OutputT>) outputs.get(mainOutputTag)));

      if (sideInputs.isEmpty()) {
        DoFnOperator<InputT, OutputT> doFnOperator =
            doFnOperatorFactory.createDoFnOperator(
                doFn,
                context.getCurrentTransform().getFullName(),
                sideInputs,
                mainOutputTag,
                additionalOutputTags,
                context,
                windowingStrategy,
                tagsToOutputTags,
                tagsToCoders,
                tagsToIds,
                inputCoder,
                keyCoder,
                new HashMap<Integer, PCollectionView<?>>() /* side-input mapping */);

        outputStream = inputDataStream
            .transform(transformName, outputTypeInformation, doFnOperator);

      } else {
        Tuple2<Map<Integer, PCollectionView<?>>, DataStream<RawUnionValue>> transformedSideInputs =
            transformSideInputs(sideInputs, context);

        DoFnOperator<InputT, OutputT> doFnOperator =
            doFnOperatorFactory.createDoFnOperator(
                doFn,
                context.getCurrentTransform().getFullName(),
                sideInputs,
                mainOutputTag,
                additionalOutputTags,
                context,
                windowingStrategy,
                tagsToOutputTags,
                tagsToCoders,
                tagsToIds,
                inputCoder,
                keyCoder,
                transformedSideInputs.f0);

        if (stateful) {
          // we have to manually contruct the two-input transform because we're not
          // allowed to have only one input keyed, normally.
          KeyedStream keyedStream = (KeyedStream<?, InputT>) inputDataStream;
          TwoInputTransformation<
              WindowedValue<KV<?, InputT>>,
              RawUnionValue,
              WindowedValue<OutputT>> rawFlinkTransform = new TwoInputTransformation(
              keyedStream.getTransformation(),
              transformedSideInputs.f1.broadcast().getTransformation(),
              transformName,
              doFnOperator,
              outputTypeInformation,
              keyedStream.getParallelism());

          rawFlinkTransform.setStateKeyType(keyedStream.getKeyType());
          rawFlinkTransform.setStateKeySelectors(keyedStream.getKeySelector(), null);

          outputStream = new SingleOutputStreamOperator(
              keyedStream.getExecutionEnvironment(),
              rawFlinkTransform) {
          }; // we have to cheat around the ctor being protected

          keyedStream.getExecutionEnvironment().addOperator(rawFlinkTransform);

        } else {
          outputStream = inputDataStream
              .connect(transformedSideInputs.f1.broadcast())
              .transform(transformName, outputTypeInformation, doFnOperator);
        }
      }

      context.setOutputDataStream(outputs.get(mainOutputTag), outputStream);

      for (Map.Entry<TupleTag<?>, PValue> entry : outputs.entrySet()) {
        if (!entry.getKey().equals(mainOutputTag)) {
          context.setOutputDataStream(entry.getValue(),
              outputStream.getSideOutput(tagsToOutputTags.get(entry.getKey())));
        }
      }
    }
  }

  private static class ParDoStreamingTranslator<InputT, OutputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PCollection<InputT>, PCollectionTuple>> {

    @Override
    public void translateNode(
        PTransform<PCollection<InputT>, PCollectionTuple> transform,
        FlinkStreamingTranslationContext context) {

      DoFn<InputT, OutputT> doFn;
      try {
        doFn = (DoFn<InputT, OutputT>) ParDoTranslation.getDoFn(context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      TupleTag<OutputT> mainOutputTag;
      try {
        mainOutputTag = (TupleTag<OutputT>)
            ParDoTranslation.getMainOutputTag(context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      List<PCollectionView<?>> sideInputs;
      try {
        sideInputs = ParDoTranslation.getSideInputs(context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      TupleTagList additionalOutputTags;
      try {
        additionalOutputTags = ParDoTranslation.getAdditionalOutputTags(
            context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      ParDoTranslationHelper.translateParDo(
          transform.getName(),
          doFn,
          context.getInput(transform),
          sideInputs,
          context.getOutputs(transform),
          mainOutputTag,
          additionalOutputTags.getAll(),
          context,
          new ParDoTranslationHelper.DoFnOperatorFactory<InputT, OutputT>() {
            @Override
            public DoFnOperator<InputT, OutputT> createDoFnOperator(
                DoFn<InputT, OutputT> doFn,
                String stepName,
                List<PCollectionView<?>> sideInputs,
                TupleTag<OutputT> mainOutputTag,
                List<TupleTag<?>> additionalOutputTags,
                FlinkStreamingTranslationContext context,
                WindowingStrategy<?, ?> windowingStrategy,
                Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags,
                Map<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders,
                Map<TupleTag<?>, Integer> tagsToIds,
                Coder<WindowedValue<InputT>> inputCoder,
                Coder keyCoder,
                Map<Integer, PCollectionView<?>> transformedSideInputs) {
              return new DoFnOperator<>(
                  doFn,
                  stepName,
                  inputCoder,
                  mainOutputTag,
                  additionalOutputTags,
                  new DoFnOperator.MultiOutputOutputManagerFactory<>(
                      mainOutputTag, tagsToOutputTags, tagsToCoders, tagsToIds),
                  windowingStrategy,
                  transformedSideInputs,
                  sideInputs,
                  context.getPipelineOptions(),
                  keyCoder);
            }
          });
    }
  }

  private static class SplittableProcessElementsStreamingTranslator<
      InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
      SplittableParDoViaKeyedWorkItems.ProcessElements<InputT, OutputT, RestrictionT, TrackerT>> {

    @Override
    public void translateNode(
        SplittableParDoViaKeyedWorkItems.ProcessElements<InputT, OutputT, RestrictionT, TrackerT>
            transform,
        FlinkStreamingTranslationContext context) {

      ParDoTranslationHelper.translateParDo(
          transform.getName(),
          transform.newProcessFn(transform.getFn()),
          context.getInput(transform),
          transform.getSideInputs(),
          context.getOutputs(transform),
          transform.getMainOutputTag(),
          transform.getAdditionalOutputTags().getAll(),
          context,
          new ParDoTranslationHelper.DoFnOperatorFactory<
              KeyedWorkItem<String, KV<InputT, RestrictionT>>, OutputT>() {
            @Override
            public DoFnOperator<KeyedWorkItem<String, KV<InputT, RestrictionT>>, OutputT>
                createDoFnOperator(
                DoFn<KeyedWorkItem<String, KV<InputT, RestrictionT>>, OutputT> doFn,
                String stepName,
                List<PCollectionView<?>> sideInputs,
                TupleTag<OutputT> mainOutputTag,
                List<TupleTag<?>> additionalOutputTags,
                FlinkStreamingTranslationContext context,
                WindowingStrategy<?, ?> windowingStrategy,
                Map<TupleTag<?>, OutputTag<WindowedValue<?>>> tagsToOutputTags,
                Map<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders,
                Map<TupleTag<?>, Integer> tagsToIds,
                Coder<WindowedValue<KeyedWorkItem<String, KV<InputT, RestrictionT>>>>
                    inputCoder,
                Coder keyCoder,
                Map<Integer, PCollectionView<?>> transformedSideInputs) {
              return new SplittableDoFnOperator<>(
                  doFn,
                  stepName,
                  inputCoder,
                  mainOutputTag,
                  additionalOutputTags,
                  new DoFnOperator.MultiOutputOutputManagerFactory<>(
                      mainOutputTag, tagsToOutputTags, tagsToCoders, tagsToIds),
                  windowingStrategy,
                  transformedSideInputs,
                  sideInputs,
                  context.getPipelineOptions(),
                  keyCoder);
            }
          });
    }
  }

  private static class CreateViewStreamingTranslator<ElemT, ViewT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
      CreateStreamingFlinkView.CreateFlinkPCollectionView<ElemT, ViewT>> {

    @Override
    public void translateNode(
        CreateStreamingFlinkView.CreateFlinkPCollectionView<ElemT, ViewT> transform,
        FlinkStreamingTranslationContext context) {
      // just forward
      DataStream<WindowedValue<List<ElemT>>> inputDataSet =
          context.getInputDataStream(context.getInput(transform));

      PCollectionView<ViewT> view = transform.getView();

      context.setOutputDataStream(view, inputDataSet);
    }
  }

  private static class WindowAssignTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PCollection<T>, PCollection<T>>> {

    @Override
    public void translateNode(
        PTransform<PCollection<T>, PCollection<T>> transform,
        FlinkStreamingTranslationContext context) {

      @SuppressWarnings("unchecked")
      WindowingStrategy<T, BoundedWindow> windowingStrategy =
          (WindowingStrategy<T, BoundedWindow>)
              context.getOutput(transform).getWindowingStrategy();

      TypeInformation<WindowedValue<T>> typeInfo =
          context.getTypeInfo(context.getOutput(transform));

      DataStream<WindowedValue<T>> inputDataStream =
          context.getInputDataStream(context.getInput(transform));

      WindowFn<T, ? extends BoundedWindow> windowFn = windowingStrategy.getWindowFn();

      FlinkAssignWindows<T, ? extends BoundedWindow> assignWindowsFunction =
          new FlinkAssignWindows<>(windowFn);

      SingleOutputStreamOperator<WindowedValue<T>> outputDataStream = inputDataStream
          .flatMap(assignWindowsFunction)
          .name(context.getOutput(transform).getName())
          .returns(typeInfo);

      context.setOutputDataStream(context.getOutput(transform), outputDataStream);
    }
  }

  private static class ReshuffleTranslatorStreaming<K, InputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
        PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, InputT>>>> {

    @Override
    public void translateNode(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, InputT>>> transform,
        FlinkStreamingTranslationContext context) {

      DataStream<WindowedValue<KV<K, InputT>>> inputDataSet =
          context.getInputDataStream(context.getInput(transform));

      context.setOutputDataStream(context.getOutput(transform), inputDataSet.rebalance());

    }
  }


  private static class GroupByKeyTranslator<K, InputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, Iterable<InputT>>>>> {

    @Override
    public void translateNode(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, Iterable<InputT>>>> transform,
        FlinkStreamingTranslationContext context) {

      PCollection<KV<K, InputT>> input = context.getInput(transform);

      @SuppressWarnings("unchecked")
      WindowingStrategy<?, BoundedWindow> windowingStrategy =
          (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy();

      KvCoder<K, InputT> inputKvCoder = (KvCoder<K, InputT>) input.getCoder();

      SingletonKeyedWorkItemCoder<K, InputT> workItemCoder = SingletonKeyedWorkItemCoder.of(
          inputKvCoder.getKeyCoder(),
          inputKvCoder.getValueCoder(),
          input.getWindowingStrategy().getWindowFn().windowCoder());

      DataStream<WindowedValue<KV<K, InputT>>> inputDataStream = context.getInputDataStream(input);

      WindowedValue.
          FullWindowedValueCoder<SingletonKeyedWorkItem<K, InputT>> windowedWorkItemCoder =
          WindowedValue.getFullCoder(
              workItemCoder,
              input.getWindowingStrategy().getWindowFn().windowCoder());

      CoderTypeInformation<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemTypeInfo =
          new CoderTypeInformation<>(windowedWorkItemCoder);

      DataStream<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemStream =
          inputDataStream
              .flatMap(new ToKeyedWorkItem<K, InputT>())
              .returns(workItemTypeInfo).name("ToKeyedWorkItem");

      KeyedStream<
          WindowedValue<
              SingletonKeyedWorkItem<K, InputT>>, ByteBuffer> keyedWorkItemStream = workItemStream
          .keyBy(new WorkItemKeySelector<K, InputT>(inputKvCoder.getKeyCoder()));

      SystemReduceFn<K, InputT, Iterable<InputT>, Iterable<InputT>, BoundedWindow> reduceFn =
          SystemReduceFn.buffering(inputKvCoder.getValueCoder());

      Coder<WindowedValue<KV<K, Iterable<InputT>>>> outputCoder =
          context.getCoder(context.getOutput(transform));
      TypeInformation<WindowedValue<KV<K, Iterable<InputT>>>> outputTypeInfo =
          context.getTypeInfo(context.getOutput(transform));

      TupleTag<KV<K, Iterable<InputT>>> mainTag = new TupleTag<>("main output");

      WindowDoFnOperator<K, InputT, Iterable<InputT>> doFnOperator =
          new WindowDoFnOperator<>(
              reduceFn,
              context.getCurrentTransform().getFullName(),
              (Coder) windowedWorkItemCoder,
              mainTag,
              Collections.<TupleTag<?>>emptyList(),
              new DoFnOperator.MultiOutputOutputManagerFactory<>(mainTag, outputCoder),
              windowingStrategy,
              new HashMap<Integer, PCollectionView<?>>(), /* side-input mapping */
              Collections.<PCollectionView<?>>emptyList(), /* side inputs */
              context.getPipelineOptions(),
              inputKvCoder.getKeyCoder());

      // our operator excepts WindowedValue<KeyedWorkItem> while our input stream
      // is WindowedValue<SingletonKeyedWorkItem>, which is fine but Java doesn't like it ...
      @SuppressWarnings("unchecked")
      SingleOutputStreamOperator<WindowedValue<KV<K, Iterable<InputT>>>> outDataStream =
          keyedWorkItemStream
              .transform(
                  transform.getName(),
                  outputTypeInfo,
                  (OneInputStreamOperator) doFnOperator);

      context.setOutputDataStream(context.getOutput(transform), outDataStream);

    }
  }

  private static class CombinePerKeyTranslator<K, InputT, OutputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
      PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>>> {

    @Override
    boolean canTranslate(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
        FlinkStreamingTranslationContext context) {

      // if we have a merging window strategy and side inputs we cannot
      // translate as a proper combine. We have to group and then run the combine
      // over the final grouped values.
      PCollection<KV<K, InputT>> input = context.getInput(transform);

      @SuppressWarnings("unchecked")
      WindowingStrategy<?, BoundedWindow> windowingStrategy =
          (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy();

      boolean hasNoSideInputs;
      try {
        hasNoSideInputs = CombineTranslation.getSideInputs(context.getCurrentTransform()).isEmpty();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      return windowingStrategy.getWindowFn().isNonMerging() || hasNoSideInputs;
    }

    @Override
    public void translateNode(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> transform,
        FlinkStreamingTranslationContext context) {

      PCollection<KV<K, InputT>> input = context.getInput(transform);

      @SuppressWarnings("unchecked")
      WindowingStrategy<?, BoundedWindow> windowingStrategy =
          (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy();

      KvCoder<K, InputT> inputKvCoder = (KvCoder<K, InputT>) input.getCoder();

      SingletonKeyedWorkItemCoder<K, InputT> workItemCoder = SingletonKeyedWorkItemCoder.of(
          inputKvCoder.getKeyCoder(),
          inputKvCoder.getValueCoder(),
          input.getWindowingStrategy().getWindowFn().windowCoder());

      DataStream<WindowedValue<KV<K, InputT>>> inputDataStream = context.getInputDataStream(input);

      WindowedValue.
          FullWindowedValueCoder<SingletonKeyedWorkItem<K, InputT>> windowedWorkItemCoder =
            WindowedValue.getFullCoder(
                workItemCoder,
                input.getWindowingStrategy().getWindowFn().windowCoder());

      CoderTypeInformation<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemTypeInfo =
          new CoderTypeInformation<>(windowedWorkItemCoder);

      DataStream<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemStream =
          inputDataStream
              .flatMap(new ToKeyedWorkItem<K, InputT>())
              .returns(workItemTypeInfo).name("ToKeyedWorkItem");

      KeyedStream<
            WindowedValue<
                SingletonKeyedWorkItem<K, InputT>>, ByteBuffer> keyedWorkItemStream = workItemStream
          .keyBy(new WorkItemKeySelector<K, InputT>(inputKvCoder.getKeyCoder()));

      GlobalCombineFn<? super InputT, ?, OutputT> combineFn;
      try {
        combineFn = (GlobalCombineFn<? super InputT, ?, OutputT>)
            CombineTranslation.getCombineFn(context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> reduceFn = SystemReduceFn.combining(
          inputKvCoder.getKeyCoder(),
          AppliedCombineFn.withInputCoder(
              combineFn, input.getPipeline().getCoderRegistry(), inputKvCoder));

      Coder<WindowedValue<KV<K, OutputT>>> outputCoder =
          context.getCoder(context.getOutput(transform));
      TypeInformation<WindowedValue<KV<K, OutputT>>> outputTypeInfo =
          context.getTypeInfo(context.getOutput(transform));

      List<PCollectionView<?>> sideInputs;
      try {
        sideInputs = CombineTranslation.getSideInputs(context.getCurrentTransform());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      if (sideInputs.isEmpty()) {

        TupleTag<KV<K, OutputT>> mainTag = new TupleTag<>("main output");
        WindowDoFnOperator<K, InputT, OutputT> doFnOperator =
            new WindowDoFnOperator<>(
                reduceFn,
                context.getCurrentTransform().getFullName(),
                (Coder) windowedWorkItemCoder,
                mainTag,
                Collections.<TupleTag<?>>emptyList(),
                new DoFnOperator.MultiOutputOutputManagerFactory<>(mainTag, outputCoder),
                windowingStrategy,
                new HashMap<Integer, PCollectionView<?>>(), /* side-input mapping */
                Collections.<PCollectionView<?>>emptyList(), /* side inputs */
                context.getPipelineOptions(),
                inputKvCoder.getKeyCoder());

        // our operator excepts WindowedValue<KeyedWorkItem> while our input stream
        // is WindowedValue<SingletonKeyedWorkItem>, which is fine but Java doesn't like it ...
        @SuppressWarnings("unchecked")
        SingleOutputStreamOperator<WindowedValue<KV<K, OutputT>>> outDataStream =
            keyedWorkItemStream.transform(
                transform.getName(), outputTypeInfo, (OneInputStreamOperator) doFnOperator);

        context.setOutputDataStream(context.getOutput(transform), outDataStream);
      } else {
        Tuple2<Map<Integer, PCollectionView<?>>, DataStream<RawUnionValue>> transformSideInputs =
            transformSideInputs(sideInputs, context);

        TupleTag<KV<K, OutputT>> mainTag = new TupleTag<>("main output");
        WindowDoFnOperator<K, InputT, OutputT> doFnOperator =
            new WindowDoFnOperator<>(
                reduceFn,
                context.getCurrentTransform().getFullName(),
                (Coder) windowedWorkItemCoder,
                mainTag,
                Collections.<TupleTag<?>>emptyList(),
                new DoFnOperator.MultiOutputOutputManagerFactory<>(mainTag, outputCoder),
                windowingStrategy,
                transformSideInputs.f0,
                sideInputs,
                context.getPipelineOptions(),
                inputKvCoder.getKeyCoder());

        // we have to manually contruct the two-input transform because we're not
        // allowed to have only one input keyed, normally.

        TwoInputTransformation<
            WindowedValue<SingletonKeyedWorkItem<K, InputT>>,
            RawUnionValue,
            WindowedValue<KV<K, OutputT>>> rawFlinkTransform = new TwoInputTransformation<>(
            keyedWorkItemStream.getTransformation(),
            transformSideInputs.f1.broadcast().getTransformation(),
            transform.getName(),
            (TwoInputStreamOperator) doFnOperator,
            outputTypeInfo,
            keyedWorkItemStream.getParallelism());

        rawFlinkTransform.setStateKeyType(keyedWorkItemStream.getKeyType());
        rawFlinkTransform.setStateKeySelectors(keyedWorkItemStream.getKeySelector(), null);

        @SuppressWarnings({ "unchecked", "rawtypes" })
        SingleOutputStreamOperator<WindowedValue<KV<K, OutputT>>> outDataStream =
            new SingleOutputStreamOperator(
                keyedWorkItemStream.getExecutionEnvironment(),
                rawFlinkTransform) {}; // we have to cheat around the ctor being protected

        keyedWorkItemStream.getExecutionEnvironment().addOperator(rawFlinkTransform);

        context.setOutputDataStream(context.getOutput(transform), outDataStream);
      }
    }
  }

  private static class GBKIntoKeyedWorkItemsTranslator<K, InputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
      PTransform<PCollection<KV<K, InputT>>, PCollection<KeyedWorkItem<K, InputT>>>> {

    @Override
    boolean canTranslate(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KeyedWorkItem<K, InputT>>> transform,
        FlinkStreamingTranslationContext context) {
      return true;
    }

    @Override
    public void translateNode(
        PTransform<PCollection<KV<K, InputT>>, PCollection<KeyedWorkItem<K, InputT>>> transform,
        FlinkStreamingTranslationContext context) {

      PCollection<KV<K, InputT>> input = context.getInput(transform);

      KvCoder<K, InputT> inputKvCoder = (KvCoder<K, InputT>) input.getCoder();

      SingletonKeyedWorkItemCoder<K, InputT> workItemCoder = SingletonKeyedWorkItemCoder.of(
          inputKvCoder.getKeyCoder(),
          inputKvCoder.getValueCoder(),
          input.getWindowingStrategy().getWindowFn().windowCoder());


      WindowedValue.
          FullWindowedValueCoder<SingletonKeyedWorkItem<K, InputT>> windowedWorkItemCoder =
          WindowedValue.getFullCoder(
              workItemCoder,
              input.getWindowingStrategy().getWindowFn().windowCoder());

      CoderTypeInformation<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemTypeInfo =
          new CoderTypeInformation<>(windowedWorkItemCoder);

      DataStream<WindowedValue<KV<K, InputT>>> inputDataStream = context.getInputDataStream(input);

      DataStream<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> workItemStream =
          inputDataStream
              .flatMap(new ToKeyedWorkItem<K, InputT>())
              .returns(workItemTypeInfo).name("ToKeyedWorkItem");

      KeyedStream<
          WindowedValue<
              SingletonKeyedWorkItem<K, InputT>>, ByteBuffer> keyedWorkItemStream = workItemStream
          .keyBy(new WorkItemKeySelector<K, InputT>(inputKvCoder.getKeyCoder()));

      context.setOutputDataStream(context.getOutput(transform), keyedWorkItemStream);
    }
  }

  private static class FlattenPCollectionTranslator<T>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
        PTransform<PCollection<T>, PCollection<T>>> {

    @Override
    public void translateNode(
        PTransform<PCollection<T>, PCollection<T>> transform,
        FlinkStreamingTranslationContext context) {
      Map<TupleTag<?>, PValue> allInputs = context.getInputs(transform);

      if (allInputs.isEmpty()) {

        // create an empty dummy source to satisfy downstream operations
        // we cannot create an empty source in Flink, therefore we have to
        // add the flatMap that simply never forwards the single element
        DataStreamSource<String> dummySource =
            context.getExecutionEnvironment().fromElements("dummy");

        DataStream<WindowedValue<T>> result = dummySource.flatMap(
            new FlatMapFunction<String, WindowedValue<T>>() {
              @Override
              public void flatMap(
                  String s,
                  Collector<WindowedValue<T>> collector) throws Exception {
                // never return anything
              }
            }).returns(
            new CoderTypeInformation<>(
                WindowedValue.getFullCoder(
                    (Coder<T>) VoidCoder.of(),
                    GlobalWindow.Coder.INSTANCE)));
        context.setOutputDataStream(context.getOutput(transform), result);

      } else {
        DataStream<T> result = null;

        // Determine DataStreams that we use as input several times. For those, we need to uniquify
        // input streams because Flink seems to swallow watermarks when we have a union of one and
        // the same stream.
        Map<DataStream<T>, Integer> duplicates = new HashMap<>();
        for (PValue input : allInputs.values()) {
          DataStream<T> current = context.getInputDataStream(input);
          Integer oldValue = duplicates.put(current, 1);
          if (oldValue != null) {
            duplicates.put(current, oldValue + 1);
          }
        }

        for (PValue input : allInputs.values()) {
          DataStream<T> current = context.getInputDataStream(input);

          final Integer timesRequired = duplicates.get(current);
          if (timesRequired > 1) {
            current = current.flatMap(new FlatMapFunction<T, T>() {
              private static final long serialVersionUID = 1L;

              @Override
              public void flatMap(T t, Collector<T> collector) throws Exception {
                collector.collect(t);
              }
            });
          }
          result = (result == null) ? current : result.union(current);
        }

        context.setOutputDataStream(context.getOutput(transform), result);
      }
    }
  }

  private static class ToKeyedWorkItem<K, InputT>
      extends RichFlatMapFunction<
      WindowedValue<KV<K, InputT>>,
      WindowedValue<SingletonKeyedWorkItem<K, InputT>>> {

    @Override
    public void flatMap(
        WindowedValue<KV<K, InputT>> inWithMultipleWindows,
        Collector<WindowedValue<SingletonKeyedWorkItem<K, InputT>>> out) throws Exception {

      // we need to wrap each one work item per window for now
      // since otherwise the PushbackSideInputRunner will not correctly
      // determine whether side inputs are ready
      //
      // this is tracked as https://issues.apache.org/jira/browse/BEAM-1850
      for (WindowedValue<KV<K, InputT>> in : inWithMultipleWindows.explodeWindows()) {
        SingletonKeyedWorkItem<K, InputT> workItem =
            new SingletonKeyedWorkItem<>(
                in.getValue().getKey(),
                in.withValue(in.getValue().getValue()));

        out.collect(in.withValue(workItem));
      }
    }
  }

  /**
   * A translator just to vend the URN. This will need to be moved to runners-core-construction-java
   * once SDF is reorganized appropriately.
   */
  private static class SplittableParDoProcessElementsTranslator
      extends PTransformTranslation.TransformPayloadTranslator.NotSerializable<
      SplittableParDoViaKeyedWorkItems.ProcessElements<?, ?, ?, ?>> {

    private SplittableParDoProcessElementsTranslator() {}

    @Override
    public String getUrn(SplittableParDoViaKeyedWorkItems.ProcessElements<?, ?, ?, ?> transform) {
      return SPLITTABLE_PROCESS_URN;
    }
  }

  /** Registers classes specialized to the Flink runner. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class FlinkTransformsRegistrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<
        ? extends Class<? extends PTransform>,
        ? extends PTransformTranslation.TransformPayloadTranslator>
    getTransformPayloadTranslators() {
      return ImmutableMap
          .<Class<? extends PTransform>, PTransformTranslation.TransformPayloadTranslator>builder()
          .put(
              CreateStreamingFlinkView.CreateFlinkPCollectionView.class,
              new CreateStreamingFlinkViewPayloadTranslator())
          .put(
              SplittableParDoViaKeyedWorkItems.ProcessElements.class,
              new SplittableParDoProcessElementsTranslator())
          .put(
              SplittableParDoViaKeyedWorkItems.GBKIntoKeyedWorkItems.class,
              new SplittableParDoGbkIntoKeyedWorkItemsPayloadTranslator())
          .build();
    }

    @Override
    public Map<String, PTransformTranslation.TransformPayloadTranslator> getTransformRehydrators() {
      return Collections.emptyMap();
    }
  }

  /**
   * A translator just to vend the URN. This will need to be moved to runners-core-construction-java
   * once SDF is reorganized appropriately.
   */
  private static class SplittableParDoProcessElementsPayloadTranslator
      extends PTransformTranslation.TransformPayloadTranslator.NotSerializable<
      SplittableParDoViaKeyedWorkItems.ProcessElements<?, ?, ?, ?>> {

    private SplittableParDoProcessElementsPayloadTranslator() {}

    @Override
    public String getUrn(SplittableParDoViaKeyedWorkItems.ProcessElements<?, ?, ?, ?> transform) {
      return SplittableParDo.SPLITTABLE_PROCESS_URN;
    }
  }

  /**
   * A translator just to vend the URN. This will need to be moved to runners-core-construction-java
   * once SDF is reorganized appropriately.
   */
  private static class SplittableParDoGbkIntoKeyedWorkItemsPayloadTranslator
      extends PTransformTranslation.TransformPayloadTranslator.NotSerializable<
      SplittableParDoViaKeyedWorkItems.GBKIntoKeyedWorkItems<?, ?>> {

    private SplittableParDoGbkIntoKeyedWorkItemsPayloadTranslator() {}

    @Override
    public String getUrn(SplittableParDoViaKeyedWorkItems.GBKIntoKeyedWorkItems<?, ?> transform) {
      return SplittableParDo.SPLITTABLE_GBKIKWI_URN;
    }
  }

  /**
   * A translator just to vend the URN.
   */
  private static class CreateStreamingFlinkViewPayloadTranslator
      extends PTransformTranslation.TransformPayloadTranslator.NotSerializable<
          CreateStreamingFlinkView.CreateFlinkPCollectionView<?, ?>> {

    private CreateStreamingFlinkViewPayloadTranslator() {}

    @Override
    public String getUrn(CreateStreamingFlinkView.CreateFlinkPCollectionView<?, ?> transform) {
      return CreateStreamingFlinkView.CREATE_STREAMING_FLINK_VIEW_URN;
    }
  }
}
