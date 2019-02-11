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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import static java.util.Objects.requireNonNull;

import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.Recommended;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.operator.StateComplexity;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.OptionalMethodBuilder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.ShuffleOperator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.PCollectionLists;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;

/**
 * Operator outputting distinct (based on {@link Object#equals}) elements.
 *
 * <h3>Builders:</h3>
 *
 * <ol>
 *   <li>{@code [named] ..................} give name to the operator [optional]
 *   <li>{@code of .......................} input dataset
 *   <li>{@code [mapped] .................} compare objects retrieved by this {@link UnaryFunction}
 *       instead of raw input elements
 *   <li>{@code [windowBy] ...............} windowing (see {@link WindowFn}), default is no
 *       windowing
 *   <li>{@code [triggeredBy] ............} defines windowing trigger, follows [windowBy] if called
 *   <li>{@code [accumulationMode] .......} windowing accumulation mode, follows [triggeredBy]
 *   <li>{@code output ...................} build output dataset
 * </ol>
 */
@Audience(Audience.Type.CLIENT)
@Recommended(
    reason =
        "Might be useful to override the default "
            + "implementation because of performance reasons"
            + "(e.g. using bloom filters), which might reduce the space complexity",
    state = StateComplexity.CONSTANT,
    repartitions = 1)
public class Distinct<InputT, KeyT> extends ShuffleOperator<InputT, KeyT, InputT>
    implements CompositeOperator<InputT, InputT> {

  /**
   * Starts building a nameless {@link Distinct} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(PCollection)
   */
  public static <InputT> MappedBuilder<InputT, InputT> of(PCollection<InputT> input) {
    return named(null).of(input);
  }

  /**
   * Starts building a named {@link Distinct} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(@Nullable String name) {
    return new Builder(name);
  }

  /** Builder for the 'of' step. */
  public interface OfBuilder extends Builders.Of {

    @Override
    <InputT> MappedBuilder<InputT, InputT> of(PCollection<InputT> input);
  }

  /** Builder for the 'mapped' step. */
  public interface MappedBuilder<InputT, KeyT> extends WindowByBuilder<InputT, KeyT> {

    /**
     * Optionally specifies a function to transform the input elements into another type among which
     * to find the distincts.
     *
     * <p>This is, while windowing will be applied on basis of original input elements, the distinct
     * operator will be carried out on the transformed elements.
     *
     * @param <KeyT> the type of the transformed elements
     * @param mapper a transform function applied to input element
     * @return the next builder to complete the setup of the {@link Distinct} operator
     */
    default <KeyT> WindowByBuilder<InputT, KeyT> mapped(UnaryFunction<InputT, KeyT> mapper) {
      return mapped(mapper, null);
    }

    <KeyT> WindowByBuilder<InputT, KeyT> mapped(
        UnaryFunction<InputT, KeyT> mapper, @Nullable TypeDescriptor<KeyT> mappedType);
  }

  /** Builder for the 'windowBy' step. */
  public interface WindowByBuilder<InputT, KeyT>
      extends Builders.WindowBy<TriggerByBuilder<InputT>>,
          OptionalMethodBuilder<WindowByBuilder<InputT, KeyT>, Builders.Output<InputT>>,
          Builders.Output<InputT> {

    @Override
    <W extends BoundedWindow> TriggerByBuilder<InputT> windowBy(WindowFn<Object, W> windowing);

    @Override
    default Builders.Output<InputT> applyIf(
        boolean cond, UnaryFunction<WindowByBuilder<InputT, KeyT>, Builders.Output<InputT>> fn) {

      return cond ? requireNonNull(fn).apply(this) : this;
    }
  }

  /** Builder for the 'triggeredBy' step. */
  public interface TriggerByBuilder<T> extends Builders.TriggeredBy<AccumulationModeBuilder<T>> {

    @Override
    AccumulationModeBuilder<T> triggeredBy(Trigger trigger);
  }

  /** Builder for the 'accumulationMode' step. */
  public interface AccumulationModeBuilder<T>
      extends Builders.AccumulationMode<WindowedOutputBuilder<T>> {

    @Override
    WindowedOutputBuilder<T> accumulationMode(WindowingStrategy.AccumulationMode accumulationMode);
  }

  /** Builder for 'windowed output' step. */
  public interface WindowedOutputBuilder<T>
      extends Builders.WindowedOutput<WindowedOutputBuilder<T>>, Builders.Output<T> {}

  private static class Builder<InputT, KeyT>
      implements OfBuilder,
          MappedBuilder<InputT, KeyT>,
          WindowByBuilder<InputT, KeyT>,
          TriggerByBuilder<InputT>,
          AccumulationModeBuilder<InputT>,
          WindowedOutputBuilder<InputT>,
          Builders.Output<InputT> {

    private final WindowBuilder<InputT> windowBuilder = new WindowBuilder<>();

    @Nullable private final String name;
    private PCollection<InputT> input;

    @SuppressWarnings("unchecked")
    private UnaryFunction<InputT, KeyT> mapper = (UnaryFunction) e -> e;

    @Nullable private TypeDescriptor<KeyT> mappedType;
    @Nullable private TypeDescriptor<InputT> outputType;
    private boolean mapped = false;

    Builder(@Nullable String name) {
      this.name = name;
    }

    @Override
    public <T> MappedBuilder<T, T> of(PCollection<T> input) {
      @SuppressWarnings("unchecked")
      final Builder<T, T> casted = (Builder) this;
      casted.input = requireNonNull(input);
      return casted;
    }

    @Override
    public <K> WindowByBuilder<InputT, K> mapped(
        UnaryFunction<InputT, K> mapper, @Nullable TypeDescriptor<K> mappedType) {

      @SuppressWarnings("unchecked")
      final Builder<InputT, K> cast = (Builder) this;
      cast.mapper = requireNonNull(mapper);
      cast.mappedType = mappedType;
      cast.mapped = true;
      return cast;
    }

    @Override
    public <W extends BoundedWindow> TriggerByBuilder<InputT> windowBy(
        WindowFn<Object, W> windowFn) {

      windowBuilder.windowBy(windowFn);
      return this;
    }

    @Override
    public AccumulationModeBuilder<InputT> triggeredBy(Trigger trigger) {
      windowBuilder.triggeredBy(trigger);
      return this;
    }

    @Override
    public WindowedOutputBuilder<InputT> accumulationMode(
        WindowingStrategy.AccumulationMode accumulationMode) {
      windowBuilder.accumulationMode(accumulationMode);
      return this;
    }

    @Override
    public WindowedOutputBuilder<InputT> withAllowedLateness(Duration allowedLateness) {
      windowBuilder.withAllowedLateness(allowedLateness);
      return this;
    }

    @Override
    public WindowedOutputBuilder<InputT> withAllowedLateness(
        Duration allowedLateness, Window.ClosingBehavior closingBehavior) {
      windowBuilder.withAllowedLateness(allowedLateness, closingBehavior);
      return this;
    }

    @Override
    public WindowedOutputBuilder<InputT> withTimestampCombiner(
        TimestampCombiner timestampCombiner) {
      windowBuilder.withTimestampCombiner(timestampCombiner);
      return this;
    }

    @Override
    public WindowedOutputBuilder<InputT> withOnTimeBehavior(Window.OnTimeBehavior behavior) {
      windowBuilder.withOnTimeBehavior(behavior);
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public PCollection<InputT> output(OutputHint... outputHints) {
      if (mapper == null) {
        this.mapper = (UnaryFunction) UnaryFunction.identity();
      }
      final Distinct<InputT, KeyT> distinct =
          new Distinct<>(
              name, mapper, outputType, mappedType, windowBuilder.getWindow().orElse(null), mapped);
      return OperatorTransform.apply(distinct, PCollectionList.of(input));
    }
  }

  private final boolean mapped;

  private Distinct(
      @Nullable String name,
      UnaryFunction<InputT, KeyT> mapper,
      @Nullable TypeDescriptor<InputT> outputType,
      @Nullable TypeDescriptor<KeyT> mappedType,
      @Nullable Window<InputT> window,
      boolean mapped) {

    super(name, outputType, mapper, mappedType, window);
    this.mapped = mapped;
  }

  @Override
  public PCollection<InputT> expand(PCollectionList<InputT> inputs) {
    PCollection<InputT> input = PCollectionLists.getOnlyElement(inputs);
    if (!mapped) {
      PCollection<KV<InputT, Void>> distinct =
          ReduceByKey.named(getName().orElse(null))
              .of(input)
              .keyBy(e -> e, input.getTypeDescriptor())
              .valueBy(e -> (Void) null, TypeDescriptors.nulls())
              .combineBy(e -> (Void) null, TypeDescriptors.nulls())
              .applyIf(
                  getWindow().isPresent(),
                  builder -> {
                    @SuppressWarnings("unchecked")
                    final ReduceByKey.WindowByInternalBuilder<InputT, InputT, Void> cast =
                        (ReduceByKey.WindowByInternalBuilder) builder;
                    return cast.windowBy(
                        getWindow()
                            .orElseThrow(
                                () ->
                                    new IllegalStateException(
                                        "Unable to resolve windowing for Distinct expansion.")));
                  })
              .output();
      return MapElements.named(getName().orElse("") + "::extract-keys")
          .of(distinct)
          .using(KV::getKey, input.getTypeDescriptor())
          .output();
    }
    return ReduceByKey.named(getName().orElse(null))
        .of(input)
        .keyBy(getKeyExtractor(), getKeyType().orElse(null))
        .valueBy(e -> e, getOutputType().orElse(null))
        .combineBy(
            e -> e.findAny().orElseThrow(() -> new IllegalStateException("Processing empty key?")),
            getOutputType().orElse(null))
        .applyIf(
            getWindow().isPresent(),
            builder -> {
              @SuppressWarnings("unchecked")
              final ReduceByKey.WindowByInternalBuilder<InputT, KeyT, InputT> cast =
                  (ReduceByKey.WindowByInternalBuilder) builder;
              return cast.windowBy(
                  getWindow()
                      .orElseThrow(
                          () ->
                              new IllegalStateException(
                                  "Unable to resolve windowing for Distinct expansion.")));
            })
        .outputValues();
  }
}
