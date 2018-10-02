/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.format;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.common.collect.Iterables;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link HadoopFormatIO} is a Transform for writing data to any sink which implements Hadoop
 * {@link OutputFormat}. For example - Cassandra, Elasticsearch, HBase, Redis, Postgres etc. {@link
 * HadoopFormatIO} has to make several performance trade-offs in connecting to {@link OutputFormat},
 * so if there is another Beam IO Transform specifically for connecting to your data sink of choice,
 * we would recommend using that one, but this IO Transform allows you to connect to many data sinks
 * that do not yet have a Beam IO Transform.
 *
 * <p>You will need to pass a Hadoop {@link Configuration} with parameters specifying how the write
 * will occur. Many properties of the Configuration are optional, and some are required for certain
 * {@link OutputFormat} classes, but the following properties must be set for all OutputFormats:
 *
 * <ul>
 *   <li>{@code mapreduce.job.outputformat.class}: The {@link OutputFormat} class used to connect to
 *       your data sink of choice.
 *   <li>{@code mapreduce.job.outputformat.key.class}: The key class passed to the {@link
 *       OutputFormat} in {@code mapreduce.job.outputformat.class}.
 *   <li>{@code mapreduce.job.outputformat.value.class}: The value class passed to the {@link
 *       OutputFormat} in {@code mapreduce.job.outputformat.class}.
 * </ul>
 *
 * <p>For example:
 *
 * <pre>{@code
 * Configuration myHadoopConfiguration = new Configuration(false);
 * // Set Hadoop OutputFormat, key and value class in configuration
 * myHadoopConfiguration.setClass(&quot;mapreduce.job.outputformat.class&quot;,
 *    MyDbOutputFormatClass, OutputFormat.class);
 * myHadoopConfiguration.setClass(&quot;mapreduce.job.outputformat.key.class&quot;,
 *    MyDbOutputFormatKeyClass, Object.class);
 * myHadoopConfiguration.setClass(&quot;mapreduce.job.outputformat.value.class&quot;,
 *    MyDbOutputFormatValueClass, Object.class);
 * }</pre>
 *
 * <p>You will need to set appropriate OutputFormat key and value class (i.e.
 * "mapreduce.job.outputformat.key.class" and "mapreduce.job.outputformat.value.class") in Hadoop
 * {@link Configuration}. If you set different OutputFormat key or value class than OutputFormat's
 * actual key or value class then, it may result in an error like "unexpected extra bytes after
 * decoding" while the decoding process of key/value object happens. Hence, it is important to set
 * appropriate OutputFormat key and value class.
 *
 * <h3>Writing using {@link HadoopFormatIO}</h3>
 *
 * <pre>{@code
 * Pipeline p = ...; // Create pipeline.
 * // Read data only with Hadoop configuration.
 * p.apply("read",
 *     HadoopFormatIO.<OutputFormatKeyClass, OutputFormatKeyClass>write()
 *              .withConfiguration(myHadoopConfiguration);
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class HadoopFormatIO {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopFormatIO.class);

  public static final String OUTPUT_FORMAT_CLASS_ATTR = MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR;
  public static final String OUTPUT_KEY_CLASS = MRJobConfig.OUTPUT_KEY_CLASS;
  public static final String OUTPUT_VALUE_CLASS = MRJobConfig.OUTPUT_VALUE_CLASS;
  public static final String NUM_REDUCES = MRJobConfig.NUM_REDUCES;
  public static final String PARTITIONER_CLASS_ATTR = MRJobConfig.PARTITIONER_CLASS_ATTR;

  /**
   * Creates an uninitialized {@link HadoopFormatIO.Write}. Before use, the {@code Write} must be
   * initialized with a HadoopFormatIO.Write#withConfiguration(HadoopConfiguration) that specifies
   * the sink.
   */
  public static <KeyT, ValueT> Write.Builder<KeyT, ValueT> write() {
    return new AutoValue_HadoopFormatIO_Write.Builder<>();
  }

  /**
   * Interface for client definition of so called {@link Configuration} "Map-Reduce" operation defined by methods {@link
   * #getConfigTransform()} and {@link #getConfigCombineFn()}
   *
   * <p>Client can define operations which will produce one particular configuration from the input
   * data by this interface. Generated configuration will be then used during writing of data into one of
   * the hadoop output formats.
   *
   * <p>This interface enables defining of special {@link Configuration} for every particular window.</p>
   *
   * @param <KeyT> Key type of writing data
   * @param <ValueT> Value type of writing data
   */
  @FunctionalInterface
  interface IConfigurationTransform<KeyT, ValueT> {

    /** Default "reduce" function for extraction of one Configuration. */
    Combine.IterableCombineFn<Configuration> DEFAULT_CONFIG_COMBINE_FN =
        Combine.IterableCombineFn.of(
            (configurations) ->
                Optional.ofNullable(Iterables.getFirst(configurations, null))
                    .orElseThrow(() -> new IllegalStateException("Any configuration found!")));

    /**
     * "Map" function which should transform one {@link KV} pair into hadoop {@link Configuration}
     *
     * <p><b>Note:</b> Default implementation of {@link #getConfigCombineFn()} requires that from
     * {@link KV} pair will be produced at least one {@link Configuration}
     *
     * @return transform function
     */
    PTransform<PCollection<? extends KV<KeyT, ValueT>>, PCollection<Configuration>>
        getConfigTransform();

    /**
     * "Reduce" function which collects all {@link Configuration}s created via {@link
     * #getConfigTransform()} and returns only one particular configuration that will be used for
     * storing of all {@link KV} pairs
     *
     * @see #DEFAULT_CONFIG_COMBINE_FN
     * @return Combine function
     */
    default CombineFnBase.GlobalCombineFn<Configuration, ?, Configuration> getConfigCombineFn() {
      return DEFAULT_CONFIG_COMBINE_FN;
    }
  }

  /**
   * @param <KeyT>
   * @param <ValueT>
   */
  private static class DefaultConfigurationTransform<KeyT, ValueT>
      implements IConfigurationTransform<KeyT, ValueT> {

    private PTransform<PCollection<? extends KV<KeyT, ValueT>>, PCollection<Configuration>>
        configTransform;

    public DefaultConfigurationTransform(
        PTransform<PCollection<? extends KV<KeyT, ValueT>>, PCollection<Configuration>>
            configTransform) {
      this.configTransform = configTransform;
    }

    @Override
    public PTransform<PCollection<? extends KV<KeyT, ValueT>>, PCollection<Configuration>>
        getConfigTransform() {
      return configTransform;
    }
  }

  /**
   * A {@link PTransform} that writes to any data sink which implements Hadoop OutputFormat. For
   * e.g. Cassandra, Elasticsearch, HBase, Redis, Postgres, etc. See the class-level Javadoc on
   * {@link HadoopFormatIO} for more information.
   *
   * @param <KeyT> Type of keys to be written.
   * @param <ValueT> Type of values to be written.
   * @see HadoopFormatIO
   */
  @AutoValue
  public abstract static class Write<KeyT, ValueT>
      extends PTransform<PCollection<KV<KeyT, ValueT>>, PDone> {

    @Nullable
    public abstract Configuration getConfiguration();

    @Nullable
    public abstract IConfigurationTransform<KeyT, ValueT> getConfigTransform();

    @AutoValue.Builder
    abstract static class Builder<KeyT, ValueT> {
      abstract Builder<KeyT, ValueT> setConfiguration(Configuration configuration);

      abstract Builder<KeyT, ValueT> setConfigTransform(
          IConfigurationTransform<KeyT, ValueT> newConfigTransform);

      abstract Write<KeyT, ValueT> build();

      /**
       * Write to the sink using the options provided by the given hadoop configuration.
       *
       * @param configuration hadoop configuration.
       * @return Created write function
       * @throws IllegalArgumentException when the configuration is null
       */
      @SuppressWarnings("unchecked")
      public Write<KeyT, ValueT> withConfiguration(Configuration configuration)
          throws IllegalArgumentException {
        checkArgument(Objects.nonNull(configuration), "Configuration can not be null");

        return setConfiguration(new Configuration(configuration)).build();
      }

      public Write<KeyT, ValueT> withConfigurationTransformation(
          PTransform<PCollection<? extends KV<KeyT, ValueT>>, PCollection<Configuration>>
              configurationTransformation) {

        setConfigTransform(() -> configurationTransformation).build();

        return setConfigTransform(new DefaultConfigurationTransform<>(configurationTransformation))
            .build();
      }

      public Write<KeyT, ValueT> withConfigurationTransformation(
          IConfigurationTransform<KeyT, ValueT> configurationTransformation) {
        return setConfigTransform(configurationTransformation).build();
      }
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {}

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      Configuration hadoopConfig = getConfiguration();
      if (hadoopConfig != null) {
        builder.addIfNotNull(
            DisplayData.item(OUTPUT_FORMAT_CLASS_ATTR, hadoopConfig.get(OUTPUT_FORMAT_CLASS_ATTR))
                .withLabel("OutputFormat Class"));
        builder.addIfNotNull(
            DisplayData.item(OUTPUT_KEY_CLASS, hadoopConfig.get(OUTPUT_KEY_CLASS))
                .withLabel("OutputFormat Key Class"));
        builder.addIfNotNull(
            DisplayData.item(OUTPUT_VALUE_CLASS, hadoopConfig.get(OUTPUT_VALUE_CLASS))
                .withLabel("OutputFormat Value Class"));
        builder.addIfNotNull(
            DisplayData.item(
                    PARTITIONER_CLASS_ATTR,
                    hadoopConfig.get(
                        PARTITIONER_CLASS_ATTR,
                        HadoopUtils.DEFAULT_PARTITIONER_CLASS_ATTR.getName()))
                .withLabel("Partitioner Class"));
      }
    }

    @Override
    public PDone expand(PCollection<KV<KeyT, ValueT>> input) {

      PCollectionView<Configuration> configView = createConfigViewAndSetupJob(input);

      return processJob(input, configView);
    }

    /**
     * Processes write job. Write job is composed from following partial steps:
     *
     * <ul>
     *   <li>Assigning of the {@link TaskID} (represented as {@link Integer}) to the {@link KV}s in
     *       {@link AssignTaskFn}
     *   <li>Grouping {@link KV}s by the {@link TaskID}
     *   <li>Writing of {@link KV} records via {@link WriteFn}
     *   <li>Global collecting of all finished Task Ids
     *   <li>Committing of whole job via {@link CommitJobFn}
     * </ul>
     *
     * @param input Collection with output data to write
     * @param configView configuration view
     * @return Successfully processed write
     */
    private PDone processJob(
        PCollection<KV<KeyT, ValueT>> input, PCollectionView<Configuration> configView) {

      TypeDescriptor<Iterable<Integer>> iterableIntType =
          TypeDescriptors.iterables(TypeDescriptors.integers());

      return PDone.in(
          input
              .apply(
                  "AssignTask",
                  ParDo.of(new AssignTaskFn<KeyT, ValueT>(configView)).withSideInputs(configView))
              .setTypeDescriptor(
                  TypeDescriptors.kvs(TypeDescriptors.integers(), input.getTypeDescriptor()))
              .apply("GroupByTaskId", GroupByKey.create())
              .apply(
                  "Write",
                  ParDo.of(new WriteFn<KeyT, ValueT>(configView)).withSideInputs(configView))
              .setTypeDescriptor(TypeDescriptors.integers())
              .apply(
                  "CollectWriteTasks",
                  Combine.globally(new IterableCombinerFn<>(TypeDescriptors.integers()))
                      .withoutDefaults())
              .setTypeDescriptor(iterableIntType)
              .apply(
                  "CommitWriteJob",
                  ParDo.of(new CommitJobFn<Integer>(configView)).withSideInputs(configView))
              .getPipeline());
    }

    /**
     * Creates configuration view based on provided Configuration ({@link
     * Write.Builder#withConfiguration(Configuration)}) or based on input data and provided
     * transformation function ({@link
     * Write.Builder#withConfigurationTransformation(IConfigurationTransform)}
     *
     * <p>Following operations are also done before configuration view creation:
     *
     * <ul>
     *   <li>Validation of configuration ({@link ConfigurationValidatorFn})
     *   <li>Validation of input data ({@link InputValidatorFn})
     *   <li>Setups start of the write job ({@link SetupJobFn})
     * </ul>
     *
     * @param input input data
     * @return Singleton view of {@link Configuration}
     */
    private PCollectionView<Configuration> createConfigViewAndSetupJob(
        PCollection<KV<KeyT, ValueT>> input) {

      TypeDescriptor<Configuration> configType = new TypeDescriptor<Configuration>() {};
      input
          .getPipeline()
          .getCoderRegistry()
          .registerCoderForType(configType, new ConfigurationCoder());

      PCollection<Configuration> config = createConfiguration(input);

      PCollectionView<Configuration> configView =
          config
              .apply("ValidateConfiguration", ParDo.of(new ConfigurationValidatorFn()))
              .apply("ValidateInput", ParDo.of(new InputValidatorFn<>(input.getTypeDescriptor())))
              .apply("SetupWriteJob", ParDo.of(new SetupJobFn()))
              .apply(View.asSingleton());

      return configView;
    }

    /**
     * Creates PCollection with one configuration based on the set source of the configuration.
     *
     * @param input input data
     * @return PCollection with single {@link Configuration}
     * @see Write.Builder#withConfiguration(Configuration)
     * @see Write.Builder#withConfigurationTransformation(IConfigurationTransform)
     */
    private PCollection<Configuration> createConfiguration(PCollection<KV<KeyT, ValueT>> input) {

      if (input.isBounded().equals(PCollection.IsBounded.UNBOUNDED)
          && input.getWindowingStrategy().equals(WindowingStrategy.globalDefault())) {
        throw new IllegalStateException(
            String.format(
                "Cannot work with %s and GLOBAL %s",
                PCollection.IsBounded.UNBOUNDED, WindowingStrategy.class.getSimpleName()));
      }

      PCollection<Configuration> config;
      if (getConfiguration() != null) {
        config =
            input
                .getPipeline()
                .apply("CreateOutputConfig", Create.<Configuration>of(getConfiguration()));
      } else if (getConfigTransform() != null) {
        config =
            input
                .apply("TransformDataIntoConfig", getConfigTransform().getConfigTransform())
                .apply(
                    Combine.globally(getConfigTransform().getConfigCombineFn()).withoutDefaults());
      } else {
        throw new IllegalStateException("Configuration reaching method was not set!");
      }

      return config;
    }
  }

  /**
   * Validates Configuration whether has all required properties and sets default values if missing.
   */
  private static class ConfigurationValidatorFn extends DoFn<Configuration, Configuration> {

    @DoFn.ProcessElement
    public void processElement(
        @DoFn.Element Configuration conf, OutputReceiver<Configuration> receiver) {
      Configuration validatedConf = new Configuration(conf);
      validateConfiguration(validatedConf);
      fillDefaultPropertiesIfMissing(validatedConf);

      receiver.output(validatedConf);
    }

    private void fillDefaultPropertiesIfMissing(Configuration conf) {
      conf.setIfUnset(NUM_REDUCES, String.valueOf(HadoopUtils.DEFAULT_NUM_REDUCERS));
      conf.setIfUnset(PARTITIONER_CLASS_ATTR, HadoopUtils.DEFAULT_PARTITIONER_CLASS_ATTR.getName());
    }

    /**
     * Validates that the mandatory configuration properties such as OutputFormat class,
     * OutputFormat key and value classes are provided in the Hadoop configuration.
     */
    private void validateConfiguration(Configuration conf) {

      checkArgument(conf != null, "Configuration can not be null");
      checkArgument(
          conf.get(OUTPUT_FORMAT_CLASS_ATTR) != null,
          "Configuration must contain \"" + OUTPUT_FORMAT_CLASS_ATTR + "\"");
      checkArgument(
          conf.get(OUTPUT_KEY_CLASS) != null,
          "Configuration must contain \"" + OUTPUT_KEY_CLASS + "\"");
      checkArgument(
          conf.get(OUTPUT_VALUE_CLASS) != null,
          "Configuration must contain \"" + OUTPUT_VALUE_CLASS + "\"");
    }
  }

  /**
   * Validates input data whether have correctly specified {@link TypeDescriptor}s of input data and
   * if the {@link TypeDescriptor}s match with output types set in the hadoop {@link Configuration}.
   *
   * @param <KeyT> Key Type of input data
   * @param <ValueT> Value Type of input data
   */
  private static class InputValidatorFn<KeyT, ValueT> extends DoFn<Configuration, Configuration> {

    private TypeDescriptor<KV<KeyT, ValueT>> inputTypeDescriptor;

    /** @param inputTypeDescriptor Type descriptor of input data */
    public InputValidatorFn(TypeDescriptor<KV<KeyT, ValueT>> inputTypeDescriptor) {
      this.inputTypeDescriptor = inputTypeDescriptor;
    }

    @SuppressWarnings("unchecked")
    @DoFn.ProcessElement
    public void processElement(
        @DoFn.Element Configuration configuration, OutputReceiver<Configuration> receiver) {
      TypeDescriptor<KeyT> outputFormatKeyClass =
          (TypeDescriptor<KeyT>) TypeDescriptor.of(configuration.getClass(OUTPUT_KEY_CLASS, null));
      TypeDescriptor<ValueT> outputFormatValueClass =
          (TypeDescriptor<ValueT>)
              TypeDescriptor.of(configuration.getClass(OUTPUT_VALUE_CLASS, null));

      checkArgument(
          inputTypeDescriptor != null,
          "Input %s must be set!",
          TypeDescriptor.class.getSimpleName());
      checkArgument(
          KV.class.equals(inputTypeDescriptor.getRawType()),
          "%s expects %s as input type.",
          Write.class.getSimpleName(),
          KV.class);
      checkArgument(
          inputTypeDescriptor.equals(
              TypeDescriptors.kvs(outputFormatKeyClass, outputFormatValueClass)),
          "%s expects following %ss: KV(Key: %s, Value: %s) but following %ss are set: KV(Key: %s, Value: %s)",
          Write.class.getSimpleName(),
          TypeDescriptor.class.getSimpleName(),
          outputFormatKeyClass.getRawType(),
          outputFormatValueClass.getRawType(),
          TypeDescriptor.class.getSimpleName(),
          inputTypeDescriptor.resolveType(KV.class.getTypeParameters()[0]),
          inputTypeDescriptor.resolveType(KV.class.getTypeParameters()[1]));

      receiver.output(configuration);
    }
  }

  /**
   * @param <KeyT>
   * @param <ValueT>
   */
  private static class TaskContext<KeyT, ValueT> {

    private RecordWriter<KeyT, ValueT> recordWriter;
    private OutputCommitter outputCommitter;
    private OutputFormat<KeyT, ValueT> outputFormatObj;
    private TaskAttemptContext taskAttemptContext;

    TaskContext(int taskId, Configuration conf) {

      JobID jobID = HadoopUtils.getJobId(conf);
      taskAttemptContext = HadoopUtils.createTaskContext(conf, jobID, taskId);
      outputFormatObj = HadoopUtils.createOutputFormatFromConfig(conf);
      outputCommitter = initOutputCommitter(outputFormatObj, conf, taskAttemptContext);
      recordWriter = initRecordWriter(outputFormatObj, taskAttemptContext);
    }

    RecordWriter<KeyT, ValueT> getRecordWriter() {
      return recordWriter;
    }

    OutputCommitter getOutputCommitter() {
      return outputCommitter;
    }

    TaskAttemptContext getTaskAttemptContext() {
      return taskAttemptContext;
    }

    private RecordWriter<KeyT, ValueT> initRecordWriter(
        OutputFormat<KeyT, ValueT> outputFormatObj, TaskAttemptContext taskAttemptContext)
        throws IllegalStateException {
      try {
        LOGGER.info(
            "Creating new RecordWriter for task {} of Job with id {}.",
            taskAttemptContext.getTaskAttemptID().getTaskID().getId(),
            taskAttemptContext.getJobID().getJtIdentifier());
        return outputFormatObj.getRecordWriter(taskAttemptContext);
      } catch (InterruptedException | IOException e) {
        throw new IllegalStateException("Unable to create RecordWriter object: ", e);
      }
    }

    private static OutputCommitter initOutputCommitter(
        OutputFormat<?, ?> outputFormatObj,
        Configuration conf,
        TaskAttemptContext taskAttemptContext)
        throws IllegalStateException {
      OutputCommitter outputCommitter;
      try {
        outputCommitter = outputFormatObj.getOutputCommitter(taskAttemptContext);
        if (outputCommitter != null) {
          outputCommitter.setupJob(new JobContextImpl(conf, taskAttemptContext.getJobID()));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Unable to create OutputCommitter object: ", e);
      }

      return outputCommitter;
    }
  }

  private static class ConfigurationCoder extends AtomicCoder<Configuration> {

    @Override
    public void encode(Configuration value, OutputStream outStream) throws IOException {
      DataOutputStream dataOutputStream = new DataOutputStream(outStream);
      value.write(dataOutputStream);
      dataOutputStream.flush();
    }

    @Override
    public Configuration decode(InputStream inStream) throws IOException {
      DataInputStream dataInputStream = new DataInputStream(inStream);
      Configuration config = new Configuration(false);
      config.readFields(dataInputStream);

      return config;
    }
  }

  /**
   * Setups start of the {@link OutputFormat} job for given window. Stores id of started job in
   * configuration. This configuration is then provided as view to all workers.
   *
   * <p>Job setup should be called only once per window
   *
   * <p>{@link JobID#getJtIdentifier()} of the created job is equal to {@link
   * BoundedWindow#maxTimestamp()} millis of the current window.
   */
  private static class SetupJobFn extends DoFn<Configuration, Configuration> {

    /**
     * Creates job, sets it as running, stores jobId in configuration and sends configuration to
     * output.
     *
     * @param config received config
     * @param receiver output receiver
     * @param c process context for pane info fetching
     * @param window info about window
     */
    @DoFn.ProcessElement
    public void processElement(
        @DoFn.Element Configuration config,
        OutputReceiver<Configuration> receiver,
        ProcessContext c,
        BoundedWindow window) {

      Configuration hadoopConf = new Configuration(config);

      JobID jobId = HadoopUtils.createJobId(String.valueOf(window.maxTimestamp().getMillis()));

      hadoopConf.set(MRJobConfig.ID, jobId.getJtIdentifier());

      setupJob(jobId, hadoopConf);

      receiver.output(new Configuration(hadoopConf));

      LOGGER.info(
          "Job with id {} successfully configured from window with max timestamp {} and pane with index {}, is first: {}.",
          jobId.getJtIdentifier(),
          window.maxTimestamp(),
          c.pane().getIndex(),
          c.pane().isFirst());
    }

    /**
     * Setups the hadoop write job as running
     *
     * @param jobId jobId
     * @param conf hadoop configuration
     */
    private void setupJob(JobID jobId, Configuration conf) {
      try {

        // TODO handle windowing
        TaskAttemptContext setupTaskContext = HadoopUtils.createSetupTaskContext(conf, jobId);
        OutputFormat<?, ?> jobOutputFormat = HadoopUtils.createOutputFormatFromConfig(conf);
        jobOutputFormat.checkOutputSpecs(setupTaskContext);
        jobOutputFormat.getOutputCommitter(setupTaskContext).setupJob(setupTaskContext);

      } catch (Exception e) {
        throw new RuntimeException("Unable to setup job.", e);
      }
    }
  }

  /**
   * Commits whole write job. This function must be called only once for one write job.
   *
   * @param <T> type of TaskId identifier
   */
  private static class CommitJobFn<T> extends DoFn<Iterable<T>, Void> {

    PCollectionView<Configuration> configView;

    CommitJobFn(PCollectionView<Configuration> configView) {
      this.configView = configView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

      Configuration config = c.sideInput(configView);
      cleanupJob(config);
    }

    /**
     * Commits whole write job
     *
     * @param config hadoop config
     */
    private void cleanupJob(Configuration config) {
      // TODO only for last pane of the window
      JobID jobID = HadoopUtils.getJobId(config);
      TaskAttemptContext cleanupTaskContext = HadoopUtils.createCleanupTaskContext(config, jobID);
      OutputFormat<?, ?> outputFormat = HadoopUtils.createOutputFormatFromConfig(config);
      try {
        OutputCommitter outputCommitter = outputFormat.getOutputCommitter(cleanupTaskContext);
        outputCommitter.commitJob(cleanupTaskContext);
      } catch (Exception e) {
        throw new RuntimeException("Unable to commit job.", e);
      }
    }
  }

  /**
   * Assigns {@link TaskID#getId()} to the given pair of key and value. {@link TaskID} is later used
   * for writing the pair to hadoop file.
   *
   * @param <KeyT> Type of key
   * @param <ValueT> Type of value
   */
  private static class AssignTaskFn<KeyT, ValueT>
      extends DoFn<KV<KeyT, ValueT>, KV<Integer, KV<KeyT, ValueT>>> {

    /** Cache of created TaskIDs for given bundle */
    private Map<Integer, TaskID> partitionToTaskContext = new HashMap<>();

    PCollectionView<Configuration> configView;
    private Partitioner<KeyT, ValueT> partitioner;
    private Integer reducersCount;
    private JobID jobId;

    /**
     * Needs configuration view of given window
     *
     * @param configView configuration view
     */
    AssignTaskFn(PCollectionView<Configuration> configView) {
      this.configView = configView;
    }

    /** Deletes cached fields used in previous bundle */
    @StartBundle
    public void startBundle() {
      partitioner = null;
      reducersCount = null;
      jobId = null;
    }

    @ProcessElement
    public void processElement(
        @Element KV<KeyT, ValueT> element,
        OutputReceiver<KV<Integer, KV<KeyT, ValueT>>> receiver,
        ProcessContext c) {

      Configuration config = c.sideInput(configView);

      TaskID taskID = createTaskIDForKV(element, config);
      int taskId = taskID.getId();
      receiver.output(KV.of(taskId, element));
    }

    /**
     * Creates or reuses existing {@link TaskID} for given record.
     *
     * <p>The {@link TaskID} creation is based on the calculation hash function of {@code KeyT} of
     * the pair via {@link Partitioner} (stored in configuration)
     *
     * @param kv keyvalue pair which should be written
     * @param config hadoop configuration
     * @return TaskID assigned to given record
     */
    private TaskID createTaskIDForKV(KV<KeyT, ValueT> kv, Configuration config) {
      int taskContextKey =
          getPartitioner(config).getPartition(kv.getKey(), kv.getValue(), getReducersCount(config));

      return partitionToTaskContext.computeIfAbsent(
          taskContextKey, (key) -> HadoopUtils.createTaskID(getJobId(config), key));
    }

    private JobID getJobId(Configuration config) {
      if (jobId == null) {
        jobId = HadoopUtils.getJobId(config);
      }
      return jobId;
    }

    private int getReducersCount(Configuration config) {
      if (reducersCount == null) {
        reducersCount = HadoopUtils.getReducersCount(config);
      }
      return reducersCount;
    }

    private Partitioner<KeyT, ValueT> getPartitioner(Configuration config) {
      if (partitioner == null) {
        partitioner = HadoopUtils.getPartitioner(config);
      }
      return partitioner;
    }
  }

  /**
   * Writes all {@link KV}s pair for given {@link TaskID} (Task Id determines partition of writing).
   *
   * <p>For every {@link TaskID} are executed following steps:
   *
   * <ul>
   *   <li>Creation of {@link TaskContext} during first pane processing.
   *   <li>Writing of every single {@link KV} pair via {@link RecordWriter}.
   *   <li>Committing of task after processing of the last pane for given {@link TaskContext}
   * </ul>
   *
   * @param <KeyT> Type of key
   * @param <ValueT> Type of value
   */
  private static class WriteFn<KeyT, ValueT>
      extends DoFn<KV<Integer, Iterable<KV<KeyT, ValueT>>>, Integer> {

    private Map<Integer, TaskContext<KeyT, ValueT>> taskIdContextMap;
    private PCollectionView<Configuration> configView;

    WriteFn(PCollectionView<Configuration> configView) {
      this.configView = configView;
    }

    /** Deletes cached map from previous bundle */
    @StartBundle
    public void setup() {
      taskIdContextMap = new HashMap<>();
    }

    @ProcessElement
    public void processElement(
        @Element KV<Integer, Iterable<KV<KeyT, ValueT>>> element, ProcessContext c, BoundedWindow b)
        throws IOException, InterruptedException {

      Configuration conf = c.sideInput(configView);

      Integer taskID = element.getKey();

      TaskContext<KeyT, ValueT> taskContext;
      if (c.pane().isFirst()) {
        taskContext = setupTask(taskID, conf);
      } else {
        taskContext = getTask(taskID, conf);
      }

      write(element.getValue(), taskContext);
      if (c.pane().isLast()) {
        taskContext.getOutputCommitter().commitTask(taskContext.getTaskAttemptContext());
        c.output(taskID);
      }
    }

    /**
     * Writes all {@link KV} pairs for given {@link TaskID}
     *
     * @param writeKVs Iterable of pairs to write
     * @param taskContext taskContext
     * @throws IOException if write problems occurred
     * @throws InterruptedException if the write thread was interrupted
     */
    private void write(Iterable<KV<KeyT, ValueT>> writeKVs, TaskContext<KeyT, ValueT> taskContext)
        throws IOException, InterruptedException {

      // write and close
      RecordWriter<KeyT, ValueT> recordWriter = taskContext.getRecordWriter();
      for (KV<KeyT, ValueT> kv : Objects.requireNonNull(writeKVs)) {
        recordWriter.write(kv.getKey(), kv.getValue());
      }
      recordWriter.close(taskContext.getTaskAttemptContext());
    }

    /**
     * Creates {@link TaskContext} and setups write for given {@code taskId}.
     *
     * @param taskId id of the write Task
     * @param conf hadoop configuration
     * @return created TaskContext
     * @throws IOException if the setup of the write task failed
     */
    private TaskContext<KeyT, ValueT> setupTask(Integer taskId, Configuration conf)
        throws IOException {

      checkArgument(
          !taskIdContextMap.containsKey(taskId),
          "Task with id %s of job %s was already set up!",
          taskId,
          HadoopUtils.getJobId(conf).getJtIdentifier());

      TaskContext<KeyT, ValueT> taskContext = new TaskContext<>(taskId, conf);
      taskIdContextMap.put(taskId, taskContext);

      taskContext.getOutputCommitter().setupTask(taskContext.getTaskAttemptContext());

      return taskContext;
    }

    /**
     * Fetches already existing (and setup) task from the map. Existence of the {@link TaskContext}
     * should be ensured during first pane key execution.
     *
     * @param taskID taskId of existing {@link TaskContext}
     * @param conf hadoop configuration
     * @return existing {@link TaskContext}
     */
    private TaskContext<KeyT, ValueT> getTask(Integer taskID, Configuration conf) {
      checkArgument(
          taskIdContextMap.containsKey(taskID),
          "Unable to process write task with id %s of Job with id %s. Reason: Task was not properly set.",
          taskID,
          HadoopUtils.getJobId(conf).getJtIdentifier());

      return taskIdContextMap.get(taskID);
    }
  }
}
