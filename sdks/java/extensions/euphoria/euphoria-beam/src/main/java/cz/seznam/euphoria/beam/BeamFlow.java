/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.accumulators.VoidAccumulatorProvider;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.util.Settings;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link Flow} that can be used in Euphoria operator constructions
 * and integrates seamlessly with Beam.
 */
public class BeamFlow extends Flow {

  /**
   * Create unnamed {@link BeamFlow}.
   * @return constructed flow
   */
  public static BeamFlow create() {
    return new BeamFlow(null, new Settings());
  }

  /**
   * Create flow with given name.
   * @param name name of the created flow
   * @return constructed flow
   */
  public static BeamFlow create(String name) {
    return new BeamFlow(name, new Settings());
  }

  /**
   * Create flow with given name and settings.
   * @param name name of the created flow
   * @param settings settings to be used
   * @return constructed flow
   */
  public static BeamFlow create(String name, Settings settings) {
    return new BeamFlow(name, settings);
  }

  /**
   * Create unnamed flow with given settings.
   * @param settings settings to be used
   * @return constructed flow
   */
  public static BeamFlow create(Settings settings) {
    return new BeamFlow(null, settings);
  }


  private final transient Map<PCollection<?>, Dataset<?>> wrapped = new HashMap<>();
  private Duration allowedLateness = Duration.ZERO;
  private AccumulatorProvider.Factory accumulatorFactory = VoidAccumulatorProvider.getFactory();
  private transient BeamExecutorContext context;

  /**
   * Construct the {@link BeamFlow}.
   * @param name name of the flow (optional)
   * @param settings settings to be used
   */
  BeamFlow(
      @Nullable String name,
      Settings settings) {

    super(name, settings);
  }

  /**
   * Set {@link AccumulatorProvider.Factory} to be used for accumulators.
   * @param accumulatorFactory the factory to use
   * @return this
   */
  public BeamFlow setAccumulatorProvider(AccumulatorProvider.Factory accumulatorFactory) {
    this.accumulatorFactory = accumulatorFactory;
    return this;
  }


  /**
   * Convert this flow to new {@link Pipeline}. The pipeline can then be used
   * to adding additional Beam transformations.
   * @param options options of the new {@link Pipeline}
   * @return the pipeline that represents transformations of this flow
   */
  public Pipeline asPipeline(PipelineOptions options) {
    return FlowTranslator.toPipeline(
        this, accumulatorFactory, options, getSettings(),
        org.joda.time.Duration.millis(allowedLateness.toMillis()));
  }

  /**
   * Write transformations of this flow to given {@link Pipeline}.
   */
  @SuppressWarnings("unchecked")
  public void into(Pipeline pipeline) {
    final DAG<Operator<?, ?>> dag = FlowTranslator.toDAG(this);

    if (context != null) {
      throw new IllegalStateException("The flow can be translated to Pipeline only once!");
    }
    context = new BeamExecutorContext(
        dag, accumulatorFactory, pipeline, getSettings(),
        org.joda.time.Duration.millis(allowedLateness.toMillis()));

    wrapped.forEach((col, ds) -> {
      context.setPCollection((Dataset) ds, (PCollection) col);
    });

    FlowTranslator.updateContextBy(dag, context);
  }

  /**
   * Wrap given {@link PCollection} as {@link Dataset} into this flow.
   * @param <T> type parameter
   * @param coll the collection
   * @return wrapped {@link Dataset}
   */
  @SuppressWarnings("unchecked")
  public <T> Dataset<T> wrapped(PCollection<T> coll) {
    Dataset<T> current = (Dataset) wrapped.get(coll);
    if (current == null) {
      current = newDataset(coll);
    }
    return current;
  }

  /**
   * Return raw Beam's {@link PCollection} represented by given {@link Dataset}.
   * @param <T> type parameter
   * @param dataset dataset to return Beam's representation for
   * @return {@link PCollection} represented by given dataset
   */
  public <T> PCollection<T> unwrapped(Dataset<T> dataset) {
    Operator<?, T> producer = dataset.getProducer();
    if (producer != null) {
      dataset = producer.output();
    }
    Dataset<T> search = dataset;
    return context.getPCollection(search).orElseThrow(() ->
        new IllegalArgumentException("Dataset " + search + " was not created by this flow!"));
  }

  private <T> Dataset<T> newDataset(PCollection<T> coll) {
    Operator<?, T> wrap = new WrappedPCollectionOperator<>(this, coll);
    super.add(wrap);
    return wrap.output();
  }

  @Override
  public <IN, OUT, T extends Operator<IN, OUT>> T add(T t) {
    return super.add(t);
  }

}
