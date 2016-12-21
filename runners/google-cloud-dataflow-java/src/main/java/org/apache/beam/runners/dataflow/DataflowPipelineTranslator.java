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
package org.apache.beam.runners.dataflow;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray;
import static org.apache.beam.sdk.util.StringUtils.byteArrayToJsonString;
import static org.apache.beam.sdk.util.StringUtils.jsonStringToByteArray;
import static org.apache.beam.sdk.util.Structs.addBoolean;
import static org.apache.beam.sdk.util.Structs.addDictionary;
import static org.apache.beam.sdk.util.Structs.addList;
import static org.apache.beam.sdk.util.Structs.addLong;
import static org.apache.beam.sdk.util.Structs.addObject;
import static org.apache.beam.sdk.util.Structs.addString;
import static org.apache.beam.sdk.util.Structs.getString;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.dataflow.model.AutoscalingSettings;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.Disk;
import com.google.api.services.dataflow.model.Environment;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.Step;
import com.google.api.services.dataflow.model.WorkerPool;
import com.google.common.base.Supplier;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.DataflowRunner.GroupByKeyAndSortValuesOnly;
import org.apache.beam.runners.dataflow.internal.ReadTranslator;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.DoFnInfo;
import org.apache.beam.runners.dataflow.util.OutputReference;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypedPValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DataflowPipelineTranslator} knows how to translate {@link Pipeline} objects
 * into Cloud Dataflow Service API {@link Job}s.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DataflowPipelineTranslator {
  // Must be kept in sync with their internal counterparts.
  private static final Logger LOG = LoggerFactory.getLogger(DataflowPipelineTranslator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * A map from {@link PTransform} subclass to the corresponding
   * {@link TransformTranslator} to use to translate that transform.
   *
   * <p>A static map that contains system-wide defaults.
   */
  private static Map<Class, TransformTranslator> transformTranslators =
      new HashMap<>();

  /** Provided configuration options. */
  private final DataflowPipelineOptions options;

  /**
   * Constructs a translator from the provided options.
   *
   * @param options Properties that configure the translator.
   *
   * @return The newly created translator.
   */
  public static DataflowPipelineTranslator fromOptions(
      DataflowPipelineOptions options) {
    return new DataflowPipelineTranslator(options);
  }

  private DataflowPipelineTranslator(DataflowPipelineOptions options) {
    this.options = options;
  }

  /**
   * Translates a {@link Pipeline} into a {@code JobSpecification}.
   */
  public JobSpecification translate(
      Pipeline pipeline,
      DataflowRunner runner,
      List<DataflowPackage> packages) {

    Translator translator = new Translator(pipeline, runner);
    Job result = translator.translate(packages);
    return new JobSpecification(result, Collections.unmodifiableMap(translator.stepNames));
  }

  /**
   * The result of a job translation.
   *
   * <p>Used to pass the result {@link Job} and any state that was used to construct the job that
   * may be of use to other classes (eg the {@link PTransform} to StepName mapping).
   */
  public static class JobSpecification {
    private final Job job;
    private final Map<AppliedPTransform<?, ?, ?>, String> stepNames;

    public JobSpecification(Job job, Map<AppliedPTransform<?, ?, ?>, String> stepNames) {
      this.job = job;
      this.stepNames = stepNames;
    }

    public Job getJob() {
      return job;
    }

    /**
     * Returns the mapping of {@link AppliedPTransform AppliedPTransforms} to the internal step
     * name for that {@code AppliedPTransform}.
     */
    public Map<AppliedPTransform<?, ?, ?>, String> getStepNames() {
      return stepNames;
    }
  }

  /**
   * Renders a {@link Job} as a string.
   */
  public static String jobToString(Job job) {
    try {
      return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(job);
    } catch (JsonProcessingException exc) {
      throw new IllegalStateException("Failed to render Job as String.", exc);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Records that instances of the specified PTransform class
   * should be translated by default by the corresponding
   * {@link TransformTranslator}.
   */
  public static <TransformT extends PTransform> void registerTransformTranslator(
      Class<TransformT> transformClass,
      TransformTranslator<? extends TransformT> transformTranslator) {
    if (transformTranslators.put(transformClass, transformTranslator) != null) {
      throw new IllegalArgumentException(
          "defining multiple translators for " + transformClass);
    }
  }

  /**
   * Returns the {@link TransformTranslator} to use for instances of the
   * specified PTransform class, or null if none registered.
   */
  public <TransformT extends PTransform>
      TransformTranslator<TransformT> getTransformTranslator(Class<TransformT> transformClass) {
    return transformTranslators.get(transformClass);
  }

  /**
   * A {@link TransformTranslator} knows how to translate a particular subclass of {@link
   * PTransform} for the Cloud Dataflow service. It does so by mutating the {@link Translator}.
   */
  public interface TransformTranslator<TransformT extends PTransform> {
    void translate(TransformT transform, Translator context);
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Translates a Pipeline into the Dataflow representation.
   *
   * <p>For internal use only.
   */
  public class Translator extends PipelineVisitor.Defaults {
    /**
     * An id generator to be used when giving unique ids for pipeline level constructs.
     * This is purposely wrapped inside of a {@link Supplier} to prevent the incorrect
     * usage of the {@link AtomicLong} that is contained.
     */
    private final Supplier<Long> idGenerator = new Supplier<Long>() {
      private final AtomicLong generator = new AtomicLong(1L);
      @Override
      public Long get() {
        return generator.getAndIncrement();
      }
    };

    /** The Pipeline to translate. */
    private final Pipeline pipeline;

    /** The runner which will execute the pipeline. */
    private final DataflowRunner runner;

    /** The Cloud Dataflow Job representation. */
    private final Job job = new Job();

    /**
     * A Map from AppliedPTransform to their unique Dataflow step names.
     */
    private final Map<AppliedPTransform<?, ?, ?>, String> stepNames = new HashMap<>();

    /**
     * A Map from PValues to their output names used by their producer
     * Dataflow steps.
     */
    private final Map<POutput, String> outputNames = new HashMap<>();

    /**
     * A Map from PValues to the Coders used for them.
     */
    private final Map<POutput, Coder<?>> outputCoders = new HashMap<>();

    /**
     * The transform currently being applied.
     */
    private AppliedPTransform<?, ?, ?> currentTransform;

    /**
     * Constructs a Translator that will translate the specified
     * Pipeline into Dataflow objects.
     */
    public Translator(Pipeline pipeline, DataflowRunner runner) {
      this.pipeline = pipeline;
      this.runner = runner;
    }

    /**
     * Translates this Translator's pipeline onto its writer.
     * @return a Job definition filled in with the type of job, the environment,
     * and the job steps.
     */
    public Job translate(List<DataflowPackage> packages) {
      job.setName(options.getJobName().toLowerCase());

      Environment environment = new Environment();
      job.setEnvironment(environment);

      try {
        environment.setSdkPipelineOptions(
            MAPPER.readValue(MAPPER.writeValueAsBytes(options), Map.class));
      } catch (IOException e) {
        throw new IllegalArgumentException(
            "PipelineOptions specified failed to serialize to JSON.", e);
      }

      WorkerPool workerPool = new WorkerPool();

      if (options.isStreaming()) {
        job.setType("JOB_TYPE_STREAMING");
      } else {
        job.setType("JOB_TYPE_BATCH");
        workerPool.setDiskType(options.getWorkerDiskType());
      }

      if (options.getWorkerMachineType() != null) {
        workerPool.setMachineType(options.getWorkerMachineType());
      }

      if (options.getUsePublicIps() != null) {
        if (options.getUsePublicIps()) {
          workerPool.setIpConfiguration("WORKER_IP_PUBLIC");
        } else {
          workerPool.setIpConfiguration("WORKER_IP_PRIVATE");
        }
      }
      workerPool.setPackages(packages);
      workerPool.setNumWorkers(options.getNumWorkers());

      if (options.isStreaming()
          && (options.getExperiments() == null
              || !options.getExperiments().contains("enable_windmill_service"))) {
        // Use separate data disk for streaming.
        Disk disk = new Disk();
        disk.setDiskType(options.getWorkerDiskType());
        workerPool.setDataDisks(Collections.singletonList(disk));
      }
      if (!isNullOrEmpty(options.getZone())) {
        workerPool.setZone(options.getZone());
      }
      if (!isNullOrEmpty(options.getNetwork())) {
        workerPool.setNetwork(options.getNetwork());
      }
      if (!isNullOrEmpty(options.getSubnetwork())) {
        workerPool.setSubnetwork(options.getSubnetwork());
      }
      if (options.getDiskSizeGb() > 0) {
        workerPool.setDiskSizeGb(options.getDiskSizeGb());
      }
      AutoscalingSettings settings = new AutoscalingSettings();
      if (options.getAutoscalingAlgorithm() != null) {
        settings.setAlgorithm(options.getAutoscalingAlgorithm().getAlgorithm());
      }
      settings.setMaxNumWorkers(options.getMaxNumWorkers());
      workerPool.setAutoscalingSettings(settings);

      List<WorkerPool> workerPools = new LinkedList<>();

      workerPools.add(workerPool);
      environment.setWorkerPools(workerPools);

      if (options.getServiceAccount() != null) {
        environment.setServiceAccountEmail(options.getServiceAccount());
      }

      pipeline.traverseTopologically(this);
      return job;
    }

    public DataflowPipelineOptions getPipelineOptions() {
      return options;
    }

    public <InputT extends PInput> InputT getInput(PTransform<InputT, ?> transform) {
      return (InputT) getCurrentTransform(transform).getInput();
    }

    public <OutputT extends POutput> OutputT getOutput(PTransform<?, OutputT> transform) {
      return (OutputT) getCurrentTransform(transform).getOutput();
    }

    public String getFullName(PTransform<?, ?> transform) {
      return getCurrentTransform(transform).getFullName();
    }

    private AppliedPTransform<?, ?, ?> getCurrentTransform(PTransform<?, ?> transform) {
      checkArgument(
          currentTransform != null && currentTransform.getTransform() == transform,
          "can only be called with current transform");
      return currentTransform;
    }


    @Override
    public void leaveCompositeTransform(TransformHierarchy.Node node) {
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      PTransform<?, ?> transform = node.getTransform();
      TransformTranslator translator =
          getTransformTranslator(transform.getClass());
      if (translator == null) {
        throw new IllegalStateException(
            "no translator registered for " + transform);
      }
      LOG.debug("Translating {}", transform);
      currentTransform = node.toAppliedPTransform();
      translator.translate(transform, this);
      currentTransform = null;
    }

    @Override
    public void visitValue(PValue value, TransformHierarchy.Node producer) {
      LOG.debug("Checking translation of {}", value);
      if (value.getProducingTransformInternal() == null) {
        throw new RuntimeException(
            "internal error: expecting a PValue "
            + "to have a producingTransform");
      }
      if (!producer.isCompositeNode()) {
        // Primitive transforms are the only ones assigned step names.
        asOutputReference(value);
      }
    }

    public Step addStep(PTransform<?, ?> transform, String type) {
      String stepName = genStepName();
      if (stepNames.put(getCurrentTransform(transform), stepName) != null) {
        throw new IllegalArgumentException(
            transform + " already has a name specified");
      }
      // Start the next "steps" list item.
      List<Step> steps = job.getSteps();
      if (steps == null) {
        steps = new LinkedList<>();
        job.setSteps(steps);
      }

      Step step = new Step();
      step.setName(stepName);
      step.setKind(type);
      steps.add(step);
      addInput(step, PropertyNames.USER_NAME, getFullName(transform));
      addDisplayData(step, stepName, transform);
      return step;
    }

    public Step addStep(PTransform<?, ? extends PValue> transform, Step original) {
      Step step = original.clone();
      String stepName = step.getName();
      if (stepNames.put(getCurrentTransform(transform), stepName) != null) {
        throw new IllegalArgumentException(transform + " already has a name specified");
      }

      Map<String, Object> properties = step.getProperties();
      if (properties != null) {
        @Nullable List<Map<String, Object>> outputInfoList = null;
        try {
          // TODO: This should be done via a Structs accessor.
          @Nullable List<Map<String, Object>> list =
              (List<Map<String, Object>>) properties.get(PropertyNames.OUTPUT_INFO);
          outputInfoList = list;
        } catch (Exception e) {
          throw new RuntimeException("Inconsistent dataflow pipeline translation", e);
        }
        if (outputInfoList != null && outputInfoList.size() > 0) {
          Map<String, Object> firstOutputPort = outputInfoList.get(0);
          @Nullable String name;
          try {
            name = getString(firstOutputPort, PropertyNames.OUTPUT_NAME);
          } catch (Exception e) {
            name = null;
          }
          if (name != null) {
            registerOutputName(getOutput(transform), name);
          }
        }
      }

      List<Step> steps = job.getSteps();
      if (steps == null) {
        steps = new LinkedList<>();
        job.setSteps(steps);
      }
      steps.add(step);
      return step;
    }

    public void addEncodingInput(Step step, Coder<?> coder) {
      CloudObject encoding = SerializableUtils.ensureSerializable(coder);
      addObject(getProperties(step), PropertyNames.ENCODING, encoding);
    }

    public void addInput(Step step, String name, Boolean value) {
      addBoolean(getProperties(step), name, value);
    }

    public void addInput(Step step, String name, String value) {
      addString(getProperties(step), name, value);
    }

    public void addInput(Step step, String name, Long value) {
      addLong(getProperties(step), name, value);
    }

    public void addInput(Step step, String name, Map<String, Object> elements) {
      addDictionary(getProperties(step), name, elements);
    }

    public void addInput(Step step, String name, List<? extends Map<String, Object>> elements) {
      addList(getProperties(step), name, elements);
    }

    public void addInput(Step step, String name, PInput value) {
      if (value instanceof PValue) {
        addInput(step, name, asOutputReference((PValue) value));
      } else {
        throw new IllegalStateException("Input must be a PValue");
      }
    }

    public long addOutput(Step step, PValue value) {
      Coder<?> coder;
      if (value instanceof TypedPValue) {
        coder = ((TypedPValue<?>) value).getCoder();
        if (value instanceof PCollection) {
          // Wrap the PCollection element Coder inside a WindowedValueCoder.
          coder = WindowedValue.getFullCoder(
              coder,
              ((PCollection<?>) value).getWindowingStrategy().getWindowFn().windowCoder());
        }
      } else {
        // No output coder to encode.
        coder = null;
      }
      return addOutput(step, value, coder);
    }

    public long addValueOnlyOutput(Step step, PValue value) {
      Coder<?> coder;
      if (value instanceof TypedPValue) {
        coder = ((TypedPValue<?>) value).getCoder();
        if (value instanceof PCollection) {
          // Wrap the PCollection element Coder inside a ValueOnly
          // WindowedValueCoder.
          coder = WindowedValue.getValueOnlyCoder(coder);
        }
      } else {
        // No output coder to encode.
        coder = null;
      }
      return addOutput(step, value, coder);
    }

    public long addCollectionToSingletonOutput(Step step, PValue inputValue, PValue outputValue) {
      Coder<?> inputValueCoder =
          checkNotNull(outputCoders.get(inputValue));
      // The inputValueCoder for the input PCollection should be some
      // WindowedValueCoder of the input PCollection's element
      // coder.
      checkState(
          inputValueCoder instanceof WindowedValue.WindowedValueCoder);
      // The outputValueCoder for the output should be an
      // IterableCoder of the inputValueCoder. This is a property
      // of the backend "CollectionToSingleton" step.
      Coder<?> outputValueCoder = IterableCoder.of(inputValueCoder);
      return addOutput(step, outputValue, outputValueCoder);
    }

    /**
     * Adds an output with the given name to the previously added
     * Dataflow step, producing the specified output {@code PValue}
     * with the given {@code Coder} (if not {@code null}).
     */
    private long addOutput(Step step, PValue value, Coder<?> valueCoder) {
      long id = idGenerator.get();
      registerOutputName(value, Long.toString(id));

      Map<String, Object> properties = getProperties(step);
      @Nullable List<Map<String, Object>> outputInfoList = null;
      try {
        // TODO: This should be done via a Structs accessor.
        outputInfoList = (List<Map<String, Object>>) properties.get(PropertyNames.OUTPUT_INFO);
      } catch (Exception e) {
        throw new RuntimeException("Inconsistent dataflow pipeline translation", e);
      }
      if (outputInfoList == null) {
        outputInfoList = new ArrayList<>();
        // TODO: This should be done via a Structs accessor.
        properties.put(PropertyNames.OUTPUT_INFO, outputInfoList);
      }

      Map<String, Object> outputInfo = new HashMap<>();
      addString(outputInfo, PropertyNames.OUTPUT_NAME, Long.toString(id));
      addString(outputInfo, PropertyNames.USER_NAME, value.getName());
      if (value instanceof PCollection
          && runner.doesPCollectionRequireIndexedFormat((PCollection<?>) value)) {
        addBoolean(outputInfo, PropertyNames.USE_INDEXED_FORMAT, true);
      }
      if (valueCoder != null) {
        // Verify that encoding can be decoded, in order to catch serialization
        // failures as early as possible.
        CloudObject encoding = SerializableUtils.ensureSerializable(valueCoder);
        addObject(outputInfo, PropertyNames.ENCODING, encoding);
        outputCoders.put(value, valueCoder);
      }

      outputInfoList.add(outputInfo);
      return id;
    }

    private void addDisplayData(Step step, String stepName, HasDisplayData hasDisplayData) {
      DisplayData displayData = DisplayData.from(hasDisplayData);
      List<Map<String, Object>> list = MAPPER.convertValue(displayData, List.class);
      addList(getProperties(step), PropertyNames.DISPLAY_DATA, list);
    }

    public OutputReference asOutputReference(PValue value) {
      AppliedPTransform<?, ?, ?> transform =
          value.getProducingTransformInternal();
      String stepName = stepNames.get(transform);
      if (stepName == null) {
        throw new IllegalArgumentException(transform + " doesn't have a name specified");
      }

      String outputName = outputNames.get(value);
      if (outputName == null) {
        throw new IllegalArgumentException(
            "output " + value + " doesn't have a name specified");
      }

      return new OutputReference(stepName, outputName);
    }

    private Map<String, Object> getProperties(Step step) {
      Map<String, Object> properties = step.getProperties();
      if (properties == null) {
        properties = new HashMap<>();
        step.setProperties(properties);
      }
      return properties;
    }

    /**
     * Returns a fresh Dataflow step name.
     */
    private String genStepName() {
      return "s" + (stepNames.size() + 1);
    }

    /**
     * Records the name of the given output PValue,
     * within its producing transform.
     */
    private void registerOutputName(POutput value, String name) {
      if (outputNames.put(value, name) != null) {
        throw new IllegalArgumentException(
            "output " + value + " already has a name specified");
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  @Override
  public String toString() {
    return "DataflowPipelineTranslator#" + hashCode();
  }


  ///////////////////////////////////////////////////////////////////////////

  static {
    registerTransformTranslator(
        View.CreatePCollectionView.class,
        new TransformTranslator<View.CreatePCollectionView>() {
          @Override
          public void translate(
              View.CreatePCollectionView transform,
              Translator context) {
            translateTyped(transform, context);
          }

          private <ElemT, ViewT> void translateTyped(
              View.CreatePCollectionView<ElemT, ViewT> transform,
              Translator context) {
            Step step = context.addStep(transform, "CollectionToSingleton");
            context.addInput(step, PropertyNames.PARALLEL_INPUT, context.getInput(transform));
            context.addCollectionToSingletonOutput(
                step,
                context.getInput(transform),
                context.getOutput(transform));
          }
        });

    DataflowPipelineTranslator.registerTransformTranslator(
        Combine.GroupedValues.class,
        new DataflowPipelineTranslator.TransformTranslator<Combine.GroupedValues>() {
          @Override
          public void translate(
              Combine.GroupedValues transform,
              Translator context) {
            translateHelper(transform, context);
          }

          private <K, InputT, OutputT> void translateHelper(
              final Combine.GroupedValues<K, InputT, OutputT> transform,
              Translator context) {
            Step step = context.addStep(transform, "CombineValues");
            translateInputs(step, context.getInput(transform), transform.getSideInputs(), context);

            AppliedCombineFn<? super K, ? super InputT, ?, OutputT> fn =
                transform.getAppliedFn(
                    context.getInput(transform).getPipeline().getCoderRegistry(),
                context.getInput(transform).getCoder(),
                context.getInput(transform).getWindowingStrategy());

            context.addEncodingInput(step, fn.getAccumulatorCoder());
            context.addInput(
                step,
                PropertyNames.SERIALIZED_FN,
                byteArrayToJsonString(serializeToByteArray(fn)));
            context.addOutput(step, context.getOutput(transform));
          }
        });

    registerTransformTranslator(
        Flatten.FlattenPCollectionList.class,
        new TransformTranslator<Flatten.FlattenPCollectionList>() {
          @Override
          public void translate(
              Flatten.FlattenPCollectionList transform,
              Translator context) {
            flattenHelper(transform, context);
          }

          private <T> void flattenHelper(
              Flatten.FlattenPCollectionList<T> transform,
              Translator context) {
            Step step = context.addStep(transform, "Flatten");

            List<OutputReference> inputs = new LinkedList<>();
            for (PCollection<T> input : context.getInput(transform).getAll()) {
              inputs.add(context.asOutputReference(input));
            }
            context.addInput(step, PropertyNames.INPUTS, inputs);
            context.addOutput(step, context.getOutput(transform));
          }
        });

    registerTransformTranslator(
        GroupByKeyAndSortValuesOnly.class,
        new TransformTranslator<GroupByKeyAndSortValuesOnly>() {
          @Override
          public void translate(GroupByKeyAndSortValuesOnly transform, Translator context) {
            groupByKeyAndSortValuesHelper(transform, context);
          }

          private <K1, K2, V> void groupByKeyAndSortValuesHelper(
              GroupByKeyAndSortValuesOnly<K1, K2, V> transform, Translator context) {
            Step step = context.addStep(transform, "GroupByKey");
            context.addInput(step, PropertyNames.PARALLEL_INPUT, context.getInput(transform));
            context.addOutput(step, context.getOutput(transform));
            context.addInput(step, PropertyNames.SORT_VALUES, true);

            // TODO: Add support for combiner lifting once the need arises.
            context.addInput(step, PropertyNames.DISALLOW_COMBINER_LIFTING, true);
          }
        });

    registerTransformTranslator(
        GroupByKey.class,
        new TransformTranslator<GroupByKey>() {
          @Override
          public void translate(
              GroupByKey transform,
              Translator context) {
            groupByKeyHelper(transform, context);
          }

          private <K, V> void groupByKeyHelper(
              GroupByKey<K, V> transform,
              Translator context) {
            Step step = context.addStep(transform, "GroupByKey");
            context.addInput(step, PropertyNames.PARALLEL_INPUT, context.getInput(transform));
            context.addOutput(step, context.getOutput(transform));

            WindowingStrategy<?, ?> windowingStrategy =
                context.getInput(transform).getWindowingStrategy();
            boolean isStreaming =
                context.getPipelineOptions().as(StreamingOptions.class).isStreaming();
            boolean disallowCombinerLifting =
                !windowingStrategy.getWindowFn().isNonMerging()
                || (isStreaming && !transform.fewKeys())
                // TODO: Allow combiner lifting on the non-default trigger, as appropriate.
                || !(windowingStrategy.getTrigger() instanceof DefaultTrigger);
            context.addInput(step,
                PropertyNames.DISALLOW_COMBINER_LIFTING, disallowCombinerLifting);
            context.addInput(step,
                PropertyNames.SERIALIZED_FN,
                byteArrayToJsonString(serializeToByteArray(windowingStrategy)));
            context.addInput(step,
                PropertyNames.IS_MERGING_WINDOW_FN,
                !windowingStrategy.getWindowFn().isNonMerging());
          }
        });

    registerTransformTranslator(
        ParDo.BoundMulti.class,
        new TransformTranslator<ParDo.BoundMulti>() {
          @Override
          public void translate(
              ParDo.BoundMulti transform,
              Translator context) {
            translateMultiHelper(transform, context);
          }

          private <InputT, OutputT> void translateMultiHelper(
              ParDo.BoundMulti<InputT, OutputT> transform,
              Translator context) {
            rejectStatefulDoFn(transform.getFn());

            Step step = context.addStep(transform, "ParallelDo");
            translateInputs(step, context.getInput(transform), transform.getSideInputs(), context);
            BiMap<Long, TupleTag<?>> outputMap =
                translateOutputs(step, context.getOutput(transform), context);
            translateFn(
                step,
                transform.getFn(),
                context.getInput(transform).getWindowingStrategy(),
                transform.getSideInputs(),
                context.getInput(transform).getCoder(),
                context,
                outputMap.inverse().get(transform.getMainOutputTag()),
                outputMap);
          }
        });

    registerTransformTranslator(
        ParDo.Bound.class,
        new TransformTranslator<ParDo.Bound>() {
          @Override
          public void translate(
              ParDo.Bound transform,
              Translator context) {
            translateSingleHelper(transform, context);
          }

          private <InputT, OutputT> void translateSingleHelper(
              ParDo.Bound<InputT, OutputT> transform,
              Translator context) {
            rejectStatefulDoFn(transform.getFn());

            Step step = context.addStep(transform, "ParallelDo");
            translateInputs(step, context.getInput(transform), transform.getSideInputs(), context);
            long mainOutput = context.addOutput(step, context.getOutput(transform));
            translateFn(
                step,
                transform.getFn(),
                context.getInput(transform).getWindowingStrategy(),
                transform.getSideInputs(),
                context.getInput(transform).getCoder(),
                context,
                mainOutput,
                ImmutableMap.<Long, TupleTag<?>>of(mainOutput,
                  new TupleTag<>(PropertyNames.OUTPUT)));

          }
        });

    registerTransformTranslator(
        Window.Bound.class,
        new DataflowPipelineTranslator.TransformTranslator<Window.Bound>() {
          @Override
          public void translate(
              Window.Bound transform, Translator context) {
            translateHelper(transform, context);
          }

          private <T> void translateHelper(
              Window.Bound<T> transform, Translator context) {
            Step step = context.addStep(transform, "Bucket");
            context.addInput(step, PropertyNames.PARALLEL_INPUT, context.getInput(transform));
            context.addOutput(step, context.getOutput(transform));

            WindowingStrategy<?, ?> strategy = context.getOutput(transform).getWindowingStrategy();
            byte[] serializedBytes = serializeToByteArray(strategy);
            String serializedJson = byteArrayToJsonString(serializedBytes);
            assert Arrays.equals(serializedBytes,
                                 jsonStringToByteArray(serializedJson));
            context.addInput(step, PropertyNames.SERIALIZED_FN, serializedJson);
          }
        });

    ///////////////////////////////////////////////////////////////////////////
    // IO Translation.

    registerTransformTranslator(Read.Bounded.class, new ReadTranslator());
  }

  private static void rejectStatefulDoFn(DoFn<?, ?> doFn) {
    if (DoFnSignatures.getSignature(doFn.getClass()).isStateful()) {
    throw new UnsupportedOperationException(
        String.format(
            "Found %s annotations on %s, but %s cannot yet be used with state in the %s.",
            DoFn.StateId.class.getSimpleName(),
            doFn.getClass().getName(),
            DoFn.class.getSimpleName(),
            DataflowRunner.class.getSimpleName()));
    }
  }

  private static void translateInputs(
      Step step,
      PCollection<?> input,
      List<PCollectionView<?>> sideInputs,
      Translator context) {
    context.addInput(step, PropertyNames.PARALLEL_INPUT, input);
    translateSideInputs(step, sideInputs, context);
  }

  // Used for ParDo
  private static void translateSideInputs(
      Step step,
      List<PCollectionView<?>> sideInputs,
      Translator context) {
    Map<String, Object> nonParInputs = new HashMap<>();

    for (PCollectionView<?> view : sideInputs) {
      nonParInputs.put(
          view.getTagInternal().getId(),
          context.asOutputReference(view));
    }

    context.addInput(step, PropertyNames.NON_PARALLEL_INPUTS, nonParInputs);
  }

  private static void translateFn(
      Step step,
      DoFn fn,
      WindowingStrategy windowingStrategy,
      Iterable<PCollectionView<?>> sideInputs,
      Coder inputCoder,
      Translator context,
      long mainOutput,
      Map<Long, TupleTag<?>> outputMap) {
    context.addInput(step, PropertyNames.USER_FN, fn.getClass().getName());
    context.addInput(
        step,
        PropertyNames.SERIALIZED_FN,
        byteArrayToJsonString(
            serializeToByteArray(
                DoFnInfo.forFn(
                    fn, windowingStrategy, sideInputs, inputCoder, mainOutput, outputMap))));
  }

  private static BiMap<Long, TupleTag<?>> translateOutputs(
      Step step,
      PCollectionTuple outputs,
      Translator context) {
    ImmutableBiMap.Builder<Long, TupleTag<?>> mapBuilder = ImmutableBiMap.builder();
    for (Map.Entry<TupleTag<?>, PCollection<?>> entry
             : outputs.getAll().entrySet()) {
      TupleTag<?> tag = entry.getKey();
      PCollection<?> output = entry.getValue();
      mapBuilder.put(context.addOutput(step, output), tag);
    }
    return mapBuilder.build();
  }
}
