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
package org.apache.beam.sdk.io.hadoop.inputformat;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.inputformat.coders.WritableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AtomicDouble;

/**
<<<<<<< HEAD
 * A bounded source for any data source which implements Hadoop InputFormat. For e.g. Cassandra,
 * Elasticsearch, HBase, Redis , Postgres etc.
 * <p>
 * The Hadoop InputFormat source returns a set of {@link org.apache.beam.sdk.values.KV} key-value
 * pairs returning a {@code PCollection<KV>}.
 * <p>
 * Hadoop Configuration is mandatory for reading the data using Hadoop InputFormat source.
 * <p>
 * Hadoop {@link org.apache.hadoop.conf.Configuration Configuration} object will have to be set with
 * following properties without fail:
 * <ul>
 * <li>mapreduce.job.inputformat.class : Signifies InputFormat class required to read data from
 * source. For e.g., if user wants to read data from Cassandra using CqlInputFormat, then
 * 'mapreduce.job.inputformat.class' property must be set to CqlInputFormat.class.</li>
 * <li>key.class : Signifies output key class of InputFormat specified in
 * 'mapreduce.job.inputformat.class'. For e.g., if class CqlInputFormat is specified in
 * 'mapreduce.job.inputformat.class', then 'key.class' property must be set to Long.class.</li>
 * <li>value.class : Signifies output value class of InputFormat specified in
 * 'mapreduce.job.inputformat.class'. For e.g., if class CqlInputFormat is specified in
 * 'mapreduce.job.inputformat.class', then 'key.class' property must be set to Row.class.</li>
=======
 * A {@link HadoopInputFormatIO} is a {@link Transform} for reading data from any source which
 * implements Hadoop InputFormat. For example- Cassandra, Elasticsearch, HBase, Redis, Postgres, etc.
 * HadoopInputFormatIO has to make several performance trade-offs in connecting to InputFormat, so
 * if there is another Beam IO Transform specifically for connecting to your data source of choice,
 * we would recommend using that one, but this IO Transform allows you to connect to many data
 * sources that do not yet have a Beam IO Transform.
 * <p>
 * You will need to pass a Hadoop {@link Configuration} with parameters specifying how the read will
 * occur. Many properties of the Configuration are optional, and some are required for certain
 * InputFormat classes, but the following properties must be set for all InputFormats:
 * <ul>
 * <li>mapreduce.job.inputformat.class: The InputFormat class used to connect to your data source
 * of choice.</li>
 * <li>key.class: The key class returned by the InputFormat in
 * 'mapreduce.job.inputformat.class'.</li>
 * <li>value.class: The value class returned by the InputFormat in
 * 'mapreduce.job.inputformat.class'.</li>
>>>>>>> Modified Javadoc for HadoopInputFormatIO.
 * </ul>
 * For example:
 *
 * <pre>
 * {@code
 *   Configuration myHadoopConfiguration = new Configuration(false);
 *   // Set Hadoop InputFormat, key and value class in configuration
 *   myHadoopConfiguration.setClass("mapreduce.job.inputformat.class", InputFormatClass,
 *       InputFormat.class);
 *   myHadoopConfiguration.setClass("key.class", InputFormatKeyClass, Object.class);
 *   myHadoopConfiguration.setClass("value.class", InputFormatValueClass, Object.class);
 * }
 * </pre>
 * <p>
 * You will need to check to see if the key and value classes output by the InputFormat have a Beam
 * {@link Coder} available. If not, You can use withKeyTranslation/withValueTranslation to specify a
 * method transforming instances of those classes into another class that is supported by a Beam
 * {@link Coder}. These settings are optional and you don't need to specify translation for both key
 * and value.
 * <p>
<<<<<<< HEAD
 * For example :
 * 
=======
 * For example:
<<<<<<< HEAD
<<<<<<< HEAD
 *
>>>>>>> Added synchronization for RecordReader in HadoopInputFormatIO
 * <pre>
 *
 * HadoopInputFormatIO.Read<MyKeyType, MyValueType> read =
 *     HadoopInputFormatIO.<MyKeyType, MyValueType>read().withConfiguration(myHadoopConfiguration)
 *         .withKeyTranslation(myOutputKeyType).withValueTranslation(myOutputValueType);
=======
 * 
 * <pre> 
=======
 *
 * <pre>
>>>>>>> Temporarily removed validate class check
 * {@code
 *   SimpleFunction<InputFormatKeyClass, MyKeyClass> myOutputKeyType =
 *       new SimpleFunction<InputFormatKeyClass, MyKeyClass>() {
 *         public MyKeyClass apply(InputFormatKeyClass input) {
 *           // ...logic to transform InputFormatKeyClass to MyKeyClass
 *         }
 *       };
 *
 *   SimpleFunction<InputFormatValueClass, MyValueClass> myOutputValueType =
 *       new SimpleFunction<InputFormatValueClass, MyValueClass>() {
 *         public MyValueClass apply(InputFormatValueClass input) {
 *           // ...logic to transform InputFormatValueClass to MyValueClass
 *         }
 *       };
 * }
>>>>>>> Modified Javadoc for HadoopInputFormatIO.
 * </pre>
 *
 * <h3>Reading using HadoopInputFormatIO</h3>
 * Pipeline p = ...; // Create pipeline.
 * <P>
 * // Read data only with Hadoop configuration.
 *
 * <pre>
 * {@code
 * p.apply("read",
 *     HadoopInputFormatIO.<InputFormatKeyClass, InputFormatKeyClass>read()
 *              .withConfiguration(myHadoopConfiguration);
 * }
 * </pre>
 * <P>
 * // Read data with configuration and key translation (Example scenario: Beam Coder is not
 * available for key class hence key translation is required.).
 *
 * <pre>
 * {@code
 * p.apply("read",
 *     HadoopInputFormatIO.<MyKeyClass, InputFormatKeyClass>read()
 *              .withConfiguration(myHadoopConfiguration)
 *              .withKeyTranslation(myOutputKeyType);
 * }
 * </pre>
 * <P>
 * // Read data with configuration and value translation (Example scenario: Beam Coder is not
 * available for value class hence value translation is required.).
 *
 * <pre>
 * {@code
 * p.apply("read",
 *     HadoopInputFormatIO.<InputFormatKeyClass, MyValueClass>read()
 *              .withConfiguration(myHadoopConfiguration)
 *              .withValueTranslation(myOutputValueType);
 * }
 * </pre>
 *
 * <P>
 * // Read data with configuration, value translation and key translation (Example scenario: Beam
 * Coders are not available for both key class and value class of InputFormat hence key and value
 * translation is required.).
 *
 * <pre>
 * {@code
 * p.apply("read",
 *     HadoopInputFormatIO.<MyKeyClass, MyValueClass>read()
 *              .withConfiguration(myHadoopConfiguration)
 *              .withKeyTranslation(myOutputKeyType)
 *              .withValueTranslation(myOutputValueType);
 * }
 * </pre>
 *
 * <h3>Read data from Cassandra using HadoopInputFormatIO transform</h3>
 * <p>
 * To read data from Cassandra, {@link org.apache.cassandra.hadoop.cql3.CqlInputFormat
 * CqlInputFormat} can be used which needs following properties to be set.
 * <p>
 * Create Cassandra Hadoop configuration as follows:
 *
 * <pre>
 * {@code
 * Configuration cassandraConf = new Configuration();
 *   cassandraConf.set("cassandra.input.thrift.port", "9160");
 *   cassandraConf.set("cassandra.input.thrift.address", CassandraHostIp);
 *   cassandraConf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");
 *   cassandraConf.set("cassandra.input.keyspace", "myKeySpace");
 *   cassandraConf.set("cassandra.input.columnfamily", "myColumnFamily");
 *   cassandraConf.setClass("key.class", {@link java.lang.Long Long.class}, Object.class);
 *   cassandraConf.setClass("value.class", {@link com.datastax.driver.core.Row Row.class}, Object.class);
 *   cassandraConf.setClass("mapreduce.job.inputformat.class", {@link org.apache.cassandra.hadoop.cql3.CqlInputFormat CqlInputFormat.class}, InputFormat.class);
 *   }
 * </pre>
 * <p>
 * Call Read transform as follows:
 *
 * <pre>
 * {@code
 * PCollection<KV<Long, String>> cassandraData =
 *          p.apply("read",
 *                  HadoopInputFormatIO.<Long, String>read()
 *                      .withConfiguration(cassandraConf)
 *                      .withValueTranslation(cassandraOutputValueType);
 * }
 * </pre>
 * <p>
<<<<<<< HEAD
<<<<<<< HEAD
 * As coder is available for CqlInputFormat key class i.e. {@link java.lang.Long} , key translation
 * is not required. For CqlInputFormat value class i.e. {@link com.datastax.driver.core.Row} coder
 * is not available in Beam, user will need to provide his own translation mechanism like following:
=======
 * As default coder is available for CqlInputFormat key class i.e. {@link java.lang.Long} , key
 * translation is not required. For CqlInputFormat value class i.e.
 * {@link com.datastax.driver.core.Row}, default coder is not available in Beam, user will need to
 * provide his/her own translation mechanism like following:
<<<<<<< HEAD
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
 * 
=======
 *
>>>>>>> Added synchronization for RecordReader in HadoopInputFormatIO
=======
 * The CqlInputFormat key class is {@link java.lang.Long Long}, which has a Beam Coder. The
 * CqlInputFormat value class is {@link com.datastax.driver.core.Row Row}, which does not have a
<<<<<<< HEAD
 * Beam Coder. Rather than write a coder, we will provide our own translation method as follows:
<<<<<<< HEAD
 * 
>>>>>>> Modified Javadoc for HadoopInputFormatIO.
=======
=======
 * Beam Coder. Rather than write a new coder, you can provide your own translation method as follows:
>>>>>>> Changes as per code review by Dipti, revisited code
 *
>>>>>>> Temporarily removed validate class check
 * <pre>
 *
 * {@code
 * SimpleFunction<Row, String> cassandraOutputValueType = SimpleFunction<Row, String>()
 * {
 *    public String apply(Row row) {
 *      return row.getString('myColName');
 *    }
 * };
 * }
 * </pre>
 *
 * <h3>Read data from Elasticsearch using HadoopInputFormatIO transform</h3>
 * <p>
 * To read data from Elasticsearch, EsInputFormat can be used which needs following properties to be
 * set.
 * <p>
 * Create ElasticSearch Hadoop configuration as follows:
 *
 * <pre>
 * {@code
 * Configuration elasticSearchConf = new Configuration();
 *   elasticSearchConf.set("es.nodes", ElasticsearchHostIp);
 *   elasticSearchConf.set("es.port", "9200");
 *   elasticSearchConf.set("es.resource", "ElasticIndexName/ElasticTypeName");
 *   elasticSearchConf.setClass("key.class", {@link org.apache.hadoop.io.Text Text.class}, Object.class);
 *   elasticSearchConf.setClass("value.class", {@link org.elasticsearch.hadoop.mr.LinkedMapWritable LinkedMapWritable.class}, Object.class);
 *   elasticSearchConf.setClass("mapreduce.job.inputformat.class", {@link org.elasticsearch.hadoop.mr.EsInputFormat EsInputFormat.class}, InputFormat.class);
 * }
 * </pre>
 *
 * Call Read transform as follows:
 *
 * <pre>
 * {@code
 *   PCollection<KV<Text, LinkedMapWritable>> elasticData = p.apply("read",
 *       HadoopInputFormatIO.<Text, LinkedMapWritable>read().withConfiguration(elasticSearchConf));
 * }
 * </pre>
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
 */

public class HadoopInputFormatIO {
<<<<<<< HEAD
=======
 * 
=======
 *
>>>>>>> Added synchronization for RecordReader in HadoopInputFormatIO
 * Here no translation mechanism is required as key and value class both have default coders
 * available.
=======
 * 
=======
 *
>>>>>>> Temporarily removed validate class check
 * <p>
 * The {@link org.elasticsearch.hadoop.mr.EsInputFormat EsInputFormat} key class is
 * {@link org.apache.hadoop.io.Text Text} and value class is
 * {@link org.elasticsearch.hadoop.mr.LinkedMapWritable LinkedMapWritable}. Both key and value
 * classes have Beam Coders.
>>>>>>> Modified Javadoc for HadoopInputFormatIO.
 */

public class HadoopInputFormatIO {
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
  private static final Logger LOG = LoggerFactory.getLogger(HadoopInputFormatIO.class);

  /**
   * Creates an uninitialized HadoopInputFormatIO.Read. Before use, the {@code Read} must be
   * initialized with a HadoopInputFormatIO.Read#withConfiguration(HadoopConfiguration) that
   * specifies the source. A key/value translation may also optionally be specified using
   * HadoopInputFormatIO.Read#withKeyTranslation/HadoopInputFormatIO.Read#withValueTranslation.
   */
  public static <K, V> Read<K, V> read() {
    return new AutoValue_HadoopInputFormatIO_Read.Builder<K, V>().build();
  }

  /**
<<<<<<< HEAD
   * A {@link PTransform} that reads from for any data source which implements Hadoop InputFormat.
<<<<<<< HEAD
<<<<<<< HEAD
   * For e.g. Cassandra, Elasticsearch, HBase, Redis , Postgres etc. See the class-level Javadoc on
=======
   * For e.g. Cassandra, Elasticsearch, HBase, Redis, Postgres etc. See the class-level Javadoc on
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
=======
=======
   * A {@link PTransform} that reads from any data source which implements Hadoop InputFormat.
>>>>>>> Changes as per code review by Dipti, revisited code
   * For e.g. Cassandra, Elasticsearch, HBase, Redis, Postgres, etc. See the class-level Javadoc on
>>>>>>> Resolved most of the code review comments.
   * {@link HadoopInputFormatIO} for more information.
   *
   * @param <K> Type of keys to be read.
   * @param <V> Type of values to be read.
   * @see HadoopInputFormatIO
   */
  @AutoValue
  public abstract static class Read<K, V> extends PTransform<PBegin, PCollection<KV<K, V>>> {

    public static TypeDescriptor<?> inputFormatClass;
    public static TypeDescriptor<?> inputFormatKeyClass;
    public static TypeDescriptor<?> inputFormatValueClass;

<<<<<<< HEAD
    /**
     * Returns the Hadoop Configuration which contains specification of source.
     */
<<<<<<< HEAD
    @Nullable
    public abstract SerializableConfiguration getConfiguration();

    @Nullable
    public abstract SimpleFunction<?, K> getKeyTranslationFunction();

    @Nullable
    public abstract SimpleFunction<?, V> getValueTranslationFunction();

    @Nullable
    public abstract TypeDescriptor<K> getKeyClass();

    @Nullable
    public abstract TypeDescriptor<V> getValueClass();
=======
    @Nullable public abstract SerializableConfiguration getConfiguration();
=======
    // Returns the Hadoop Configuration which contains specification of source.
    @Nullable
    public abstract SerializableConfiguration getConfiguration();

>>>>>>> Changes as per code review by Dipti, revisited code
    @Nullable public abstract SimpleFunction<?, K> getKeyTranslationFunction();
    @Nullable public abstract SimpleFunction<?, V> getValueTranslationFunction();
    @Nullable public abstract TypeDescriptor<K> getKeyClass();
    @Nullable public abstract TypeDescriptor<V> getValueClass();
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.

    abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setConfiguration(SerializableConfiguration configuration);
<<<<<<< HEAD

      abstract Builder<K, V> setKeyTranslationFunction(SimpleFunction<?, K> function);

      abstract Builder<K, V> setValueTranslationFunction(SimpleFunction<?, V> function);

      abstract Builder<K, V> setKeyClass(TypeDescriptor<K> keyClass);

      abstract Builder<K, V> setValueClass(TypeDescriptor<V> valueClass);

=======
      abstract Builder<K, V> setKeyTranslationFunction(SimpleFunction<?, K> function);
      abstract Builder<K, V> setValueTranslationFunction(SimpleFunction<?, V> function);
      abstract Builder<K, V> setKeyClass(TypeDescriptor<K> keyClass);
      abstract Builder<K, V> setValueClass(TypeDescriptor<V> valueClass);
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
      abstract Read<K, V> build();
    }

    /**
<<<<<<< HEAD
     * Returns a new {@link HadoopInputFormatIO.Read} that will read from the source along with
=======
     * Returns a new {@link HadoopInputFormatIO.Read} that will read from the source using the
<<<<<<< HEAD
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
     * options indicated by the given configuration.
=======
     * options provided by the given configuration.
>>>>>>> Changes as per code review by Dipti, revisited code
     *
     * <p>
     * Does not modify this object.
     */
    public Read<K, V> withConfiguration(Configuration configuration) {
      validateConfiguration(configuration);
<<<<<<< HEAD
      inputFormatClass =
          TypeDescriptor.of(configuration.getClass("mapreduce.job.inputformat.class", null));
      inputFormatKeyClass = TypeDescriptor.of(configuration.getClass("key.class", null));
      inputFormatValueClass = TypeDescriptor.of(configuration.getClass("value.class", null));
      // Sets the configuration.
      Builder<K, V> builder =
          toBuilder().setConfiguration(new SerializableConfiguration(configuration));
      // Sets the output key class to InputFormat key class if withKeyTranslation() is not called
      // yet.
      if (this.getKeyClass() == null) {
        builder.setKeyClass((TypeDescriptor<K>) inputFormatKeyClass);
      }
      // Sets the output value class to InputFormat value class if withValueTranslation() is not
      // called yet.
=======
      inputFormatClass = TypeDescriptor
          .of(configuration.getClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME, null));
      inputFormatKeyClass = TypeDescriptor
          .of(configuration.getClass(HadoopInputFormatIOConstants.KEY_CLASS, null));
      inputFormatValueClass = TypeDescriptor
          .of(configuration.getClass(HadoopInputFormatIOConstants.VALUE_CLASS, null));
      Builder<K, V> builder = toBuilder()
          .setConfiguration(new SerializableConfiguration(configuration));
      /*
       * Sets the output key class to InputFormat key class if withKeyTranslation() is not called
       * yet.
       */
      if (getKeyTranslationFunction() == null) {
        builder.setKeyClass((TypeDescriptor<K>) inputFormatKeyClass);
      }
      /*
       * Sets the output value class to InputFormat value class if withValueTranslation() is not
       * called yet.
       */
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
      if (this.getValueClass() == null) {
=======
      if ( this.getValueTranslationFunction() == null) {
>>>>>>> Modifications according to code review comments.
=======
      if (this.getValueTranslationFunction() == null) {
>>>>>>> Changes for spaces, Constants file name and comments as per Stephens code review comments
=======
      if (getValueTranslationFunction() == null) {
>>>>>>> Resolved most of the code review comments.
        builder.setValueClass((TypeDescriptor<V>) inputFormatValueClass);
      }
      return builder.build();
    }
<<<<<<< HEAD
    
=======

<<<<<<< HEAD
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
    /**
     * Validates that the mandatory configuration properties such as InputFormat class, InputFormat
     * key class and InputFormat value class are provided in the given Hadoop configuration.
     */
    private void validateConfiguration(Configuration configuration) {
<<<<<<< HEAD
      checkNotNull(configuration, HadoopInputFormatIOContants.NULL_CONFIGURATION_ERROR_MSG);
<<<<<<< HEAD
      checkNotNull(configuration.get("mapreduce.job.inputformat.class"), HadoopInputFormatIOContants.MISSING_INPUTFORMAT_ERROR_MSG);
=======
      checkNotNull(configuration.get("mapreduce.job.inputformat.class"),
          HadoopInputFormatIOContants.MISSING_INPUTFORMAT_ERROR_MSG);
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
=======
      checkNotNull(configuration, HadoopInputFormatIOConstants.NULL_CONFIGURATION_ERROR_MSG);
      checkNotNull(configuration.get("mapreduce.job.inputformat.class"),
          HadoopInputFormatIOConstants.MISSING_INPUTFORMAT_ERROR_MSG);
>>>>>>> Changes for spaces, Constants file name and comments as per Stephens code review comments
      checkNotNull(configuration.get("key.class"),
          HadoopInputFormatIOConstants.MISSING_INPUTFORMAT_KEY_CLASS_ERROR_MSG);
      checkNotNull(configuration.get("value.class"),
          HadoopInputFormatIOConstants.MISSING_INPUTFORMAT_VALUE_CLASS_ERROR_MSG);
    }

    /**
=======
     /**
>>>>>>> Changes as per code review by Dipti, revisited code
     * Returns a new {@link HadoopInputFormatIO.Read} that will transform the keys read from the
     * source using the given key translation function.
     *
     * <p>
     * Does not modify this object.
     */
    public Read<K, V> withKeyTranslation(SimpleFunction<?, K> function) {
      checkNotNull(function, HadoopInputFormatIOConstants.NULL_KEY_TRANSLATIONFUNC_ERROR_MSG);
      // Sets key class to key translation function's output class type.
      return toBuilder().setKeyTranslationFunction(function)
          .setKeyClass((TypeDescriptor<K>) function.getOutputTypeDescriptor()).build();
    }

    /**
     * Returns a new {@link HadoopInputFormatIO.Read} that will transform the values read from the
     * source using the given value translation function.
     *
     * <p>
     * Does not modify this object.
     */
    public Read<K, V> withValueTranslation(SimpleFunction<?, V> function) {
      checkNotNull(function, HadoopInputFormatIOConstants.NULL_VALUE_TRANSLATIONFUNC_ERROR_MSG);
      // Sets value class to value translation function's output class type.
      return toBuilder().setValueTranslationFunction(function)
          .setValueClass((TypeDescriptor<V>) function.getOutputTypeDescriptor()).build();
    }

    @Override
    public PCollection<KV<K, V>> expand(PBegin input) {
      // Get the key and value coders based on the key and value classes.
      CoderRegistry coderRegistry = input.getPipeline().getCoderRegistry();
      Coder<K> keyCoder = getDefaultCoder(getKeyClass(), coderRegistry);
      Coder<V> valueCoder = getDefaultCoder(getValueClass(), coderRegistry);
      HadoopInputFormatBoundedSource<K, V> source = new HadoopInputFormatBoundedSource<K, V>(
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
          this.getConfiguration(), this.getKeyCoder(), this.getValueCoder(),
          this.getKeyTranslationFunction(), this.getValueTranslationFunction(), null);
=======
          this.getConfiguration(), 
          this.getKeyCoder(), 
=======
          this.getConfiguration(),
          this.getKeyCoder(),
>>>>>>> Added synchronization for RecordReader in HadoopInputFormatIO
          this.getValueCoder(),
          this.getKeyTranslationFunction(),
          this.getValueTranslationFunction(),
          null);
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
=======
          getConfiguration(),
          keyCoder,
          valueCoder,
          getKeyTranslationFunction(),
          getValueTranslationFunction());
>>>>>>> Resolved most of the code review comments.
      return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(source));
    }

    /**
     * Validates that the mandatory configuration properties such as InputFormat class, InputFormat
     * key and value classes are provided in the Hadoop configuration.
     */
    private void validateConfiguration(Configuration configuration) {
      checkNotNull(configuration, HadoopInputFormatIOConstants.NULL_CONFIGURATION_ERROR_MSG);
      checkNotNull(configuration.get("mapreduce.job.inputformat.class"),
          HadoopInputFormatIOConstants.MISSING_INPUTFORMAT_ERROR_MSG);
      checkNotNull(configuration.get("key.class"),
          HadoopInputFormatIOConstants.MISSING_INPUTFORMAT_KEY_CLASS_ERROR_MSG);
      checkNotNull(configuration.get("value.class"),
          HadoopInputFormatIOConstants.MISSING_INPUTFORMAT_VALUE_CLASS_ERROR_MSG);
    }

    /**
     * Validates inputs provided by the pipeline user before reading the data.
     */
    @Override
    public void validate(PBegin input) {
      checkNotNull(getConfiguration(),
          HadoopInputFormatIOConstants.MISSING_CONFIGURATION_ERROR_MSG);
      // Validate that the key translation input type must be same as key class of InputFormat.
      validateTranslationFunction(inputFormatKeyClass, getKeyTranslationFunction(),
          HadoopInputFormatIOConstants.WRONG_KEY_TRANSLATIONFUNC_ERROR_MSG);
      // Validate that the value translation input type must be same as value class of InputFormat.
      validateTranslationFunction(inputFormatValueClass, getValueTranslationFunction(),
          HadoopInputFormatIOConstants.WRONG_VALUE_TRANSLATIONFUNC_ERROR_MSG);
    }


    /**
     * Validates translation function given for key/value translation.
     */
    private void validateTranslationFunction(TypeDescriptor<?> inputType,
        SimpleFunction<?, ?> simpleFunction, String errorMsg) {
      if (simpleFunction != null) {
        if (!simpleFunction.getInputTypeDescriptor().equals(inputType)) {
          throw new IllegalArgumentException(
              String.format(errorMsg, inputFormatClass.getRawType(), inputType.getRawType()));
        }
      }
    }

    /**
     * Returns the default coder for a given type descriptor. Coder Registry is queried for correct
     * coder, if not found in Coder Registry then check if the type desciptor provided is of
     * Writable, then WritableCoder is returned, else exception is thrown "Cannot find coder".
     */
    @VisibleForTesting
    public <T> Coder<T> getDefaultCoder(TypeDescriptor<?> typeDesc, CoderRegistry coderRegistry) {
      Class classType = typeDesc.getRawType();
      try {
        return (Coder<T>) coderRegistry.getCoder(typeDesc);
      } catch (CannotProvideCoderException e) {
        if (Writable.class.isAssignableFrom(classType)) {
          return (Coder<T>) WritableCoder.of(classType);
        }
        throw new IllegalStateException(
            String.format(HadoopInputFormatIOConstants.CANNOT_FIND_CODER_ERROR_MSG, typeDesc)
                + e.getMessage(),
            e);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
<<<<<<< HEAD
      if (this.getConfiguration().getHadoopConfiguration() != null) {
<<<<<<< HEAD
        Iterator<Entry<String, String>> configProperties =
            this.getConfiguration().getHadoopConfiguration().iterator();
=======
        Iterator<Entry<String, String>> configProperties = this.getConfiguration()
=======
      if (getConfiguration().getHadoopConfiguration() != null) {
        Iterator<Entry<String, String>> configProperties = getConfiguration()
>>>>>>> Resolved most of the code review comments.
            .getHadoopConfiguration().iterator();
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
        while (configProperties.hasNext()) {
          Entry<String, String> property = configProperties.next();
          builder.add(DisplayData.item(property.getKey(), property.getValue())
              .withLabel(property.getKey()));
        }
      }
<<<<<<< HEAD
      builder
          .addIfNotNull(DisplayData.item("KeyClass", getKeyClass().getRawType())
              .withLabel("Output key class"))
          .addIfNotNull(DisplayData.item("ValueClass", getValueClass().getRawType())
              .withLabel("Output value class"));
      if (getKeyTranslationFunction() != null)
<<<<<<< HEAD
        builder.addIfNotNull(
            DisplayData.item("KeyTranslationSimpleFunction", getKeyTranslationFunction().toString())
                .withLabel("Key translation SimpleFunction"));
=======
        builder.addIfNotNull(DisplayData
            .item("KeyTranslation", getKeyTranslationFunction().toString())
            .withLabel("Key translation SimpleFunction"));
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
      if (getValueTranslationFunction() != null)
        builder.addIfNotNull(DisplayData
            .item("ValueTranslation", getValueTranslationFunction().toString())
            .withLabel("Value translation SimpleFunction"));

=======
>>>>>>> Resolved most of the code review comments.
    }
  }

  /**
   * Bounded source implementation for HadoopInputFormatIO.
   *
   * @param <K> Type of keys to be read.
   * @param <V> Type of values to be read.
   */
  public static class HadoopInputFormatBoundedSource<K, V> extends BoundedSource<KV<K, V>>
      implements Serializable {
    private final SerializableConfiguration conf;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final SimpleFunction<?, K> keyTranslationFunction;
    private final SimpleFunction<?, V> valueTranslationFunction;
    private final SerializableSplit inputSplit;
    private transient List<SerializableSplit> inputSplits;
    private long boundedSourceEstimatedSize = 0;
<<<<<<< HEAD

    public HadoopInputFormatBoundedSource(SerializableConfiguration conf, Coder<K> keyCoder,
        Coder<V> valueCoder) {
      this(conf, keyCoder, valueCoder, null, null, null);
    }

    public HadoopInputFormatBoundedSource(SerializableConfiguration conf, Coder<K> keyCoder,
        Coder<V> valueCoder, SimpleFunction<?, K> keyTranslationFunction,
        SimpleFunction<?, V> valueTranslationFunction, SerializableSplit inputSplit) {
=======
    private InputFormat<?, ?> inputFormatObj;
    private TaskAttemptContext taskAttemptContext;
    private transient Class<?> expectedKeyClass;
    private transient Class<?> expectedValueClass;

    HadoopInputFormatBoundedSource(
        SerializableConfiguration conf,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        SimpleFunction<?, K> keyTranslationFunction,
        SimpleFunction<?, V> valueTranslationFunction) {
      this(conf,
          keyCoder,
          valueCoder,
          keyTranslationFunction,
          valueTranslationFunction,
          null,
          null,
          null);
    }

    protected HadoopInputFormatBoundedSource(
        SerializableConfiguration conf,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        SimpleFunction<?, K> keyTranslationFunction,
        SimpleFunction<?, V> valueTranslationFunction,
        InputFormat<?, ?> inputFormatObj,
<<<<<<< HEAD
        SerializableSplit inputSplit) {
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
=======
        SerializableSplit inputSplit,
<<<<<<< HEAD
        TaskAttemptContextImpl taskAttemptContext) {
>>>>>>> Resolved most of the code review comments.
=======
        TaskAttemptContext taskAttemptContext) {
>>>>>>> Mockito commit continued
      this.conf = conf;
      this.inputSplit = inputSplit;
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.keyTranslationFunction = keyTranslationFunction;
      this.valueTranslationFunction = valueTranslationFunction;
<<<<<<< HEAD
=======
      this.inputFormatObj = inputFormatObj;
<<<<<<< HEAD
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
=======
      this.taskAttemptContext = taskAttemptContext;
>>>>>>> Resolved most of the code review comments.
    }

    public SerializableConfiguration getConfiguration() {
      return conf;
    }

    @Override
    public void validate() {
      checkNotNull(conf, HadoopInputFormatIOConstants.MISSING_CONFIGURATION_SOURCE_ERROR_MSG);
      checkNotNull(keyCoder, HadoopInputFormatIOConstants.MISSING_KEY_CODER_SOURCE_ERROR_MSG);
      checkNotNull(valueCoder, HadoopInputFormatIOConstants.MISSING_VALUE_CODER_SOURCE_ERROR_MSG);
    }

<<<<<<< HEAD
    // Indicates if the source is split or not.
    private transient boolean isSourceSplit = false;

    public boolean isSourceSplit() {
      return isSourceSplit;
    }

    public void setSourceSplit(boolean isSourceSplit) {
      this.isSourceSplit = isSourceSplit;
    }

=======
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
    @Override
    public List<BoundedSource<KV<K, V>>> splitIntoBundles(long desiredBundleSizeBytes,
        PipelineOptions options) throws Exception {
      if (inputSplit == null) {
        computeSplitsIfNecessary();
        LOG.info("Generated {} splits each of size {} ", inputSplits.size(),
            inputSplits.get(0).getSplit().getLength());
<<<<<<< HEAD
        setSourceSplit(true);
=======
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
        return Lists.transform(inputSplits,
            new Function<SerializableSplit, BoundedSource<KV<K, V>>>() {
              @Override
              public BoundedSource<KV<K, V>> apply(SerializableSplit serializableInputSplit) {
                HadoopInputFormatBoundedSource<K, V> hifBoundedSource =
                    new HadoopInputFormatBoundedSource<K, V>(conf, keyCoder, valueCoder,
<<<<<<< HEAD
                        keyTranslationFunction, valueTranslationFunction, serializableInputSplit);
=======
                        keyTranslationFunction, valueTranslationFunction, inputFormatObj,
<<<<<<< HEAD
                        serializableInputSplit);
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
=======
                        serializableInputSplit,taskAttemptContext);
>>>>>>> Resolved most of the code review comments.
                return hifBoundedSource;
              }
            });
      } else {
<<<<<<< HEAD
        setSourceSplit(true);
=======
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
        LOG.info("Not splitting source {} because source is already split.", this);
        return ImmutableList.of((BoundedSource<KV<K, V>>) this);
      }
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions po) throws Exception {
      if (inputSplit == null) {
        computeSplitsIfNecessary();
        return boundedSourceEstimatedSize;
      }
      return inputSplit.getSplit().getLength();
    }


    /**
     * This is a helper function to compute splits. This method will also calculate size of the data
     * being read. Note: This method is called exactly once, the splits are retrieved and cached
     * for further use by splitIntoBundles() and getEstimatesSizeBytes().
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
     */
    private void computeSplits() throws IOException, IllegalAccessException, InstantiationException,
        InterruptedException, ClassNotFoundException {
      Job job = Job.getInstance(conf.getHadoopConfiguration());
      List<InputSplit> splits = job.getInputFormatClass().newInstance().getSplits(job);
      if (splits == null) {
        throw new IOException(HadoopInputFormatIOContants.COMPUTESPLITS_NULL_SPLITS_ERROR_MSG);
=======
     * @throws InterruptedException 
=======
=======
     * @throws IOException
>>>>>>> Changes as per code review by Dipti, revisited code
     * @throws InterruptedException
>>>>>>> Added synchronization for RecordReader in HadoopInputFormatIO
=======
>>>>>>> Added key and value class validation and added javadoc in HIFIOWithPostgresIT.
     */
    @VisibleForTesting
<<<<<<< HEAD
    void computeSplits() throws IOException, InterruptedException{
      inputFormatObj = createInputFormat();
      List<InputSplit> splits =
          inputFormatObj.getSplits(Job.getInstance(conf.getHadoopConfiguration()));
      if (splits == null) {
<<<<<<< HEAD
        throw new IOException(HadoopInputFormatIOContants.COMPUTESPLITS_NULL_GETSPLITS_ERROR_MSG);
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
=======
        throw new IOException(HadoopInputFormatIOConstants.COMPUTESPLITS_NULL_GETSPLITS_ERROR_MSG);
>>>>>>> Changes for spaces, Constants file name and comments as per Stephens code review comments
      }
      if (splits.isEmpty()) {
        throw new IOException(HadoopInputFormatIOConstants.COMPUTESPLITS_EMPTY_SPLITS_ERROR_MSG);
      }
      boundedSourceEstimatedSize = 0;
      inputSplits = new ArrayList<SerializableSplit>();
<<<<<<< HEAD
      for (InputSplit inputSplit : splits) {
<<<<<<< HEAD
        boundedSourceEstimatedSize = boundedSourceEstimatedSize + inputSplit.getLength();
        inputSplits.add(new SerializableSplit(inputSplit));
      }
    }

=======
=======
      for (InputSplit inputSplit: splits) {
>>>>>>> Changes for spaces, Constants file name and comments as per Stephens code review comments
        if (inputSplit == null) {
          throw new IOException(HadoopInputFormatIOConstants.COMPUTESPLITS_NULL_SPLIT_ERROR_MSG);
=======
    void computeSplitsIfNecessary() throws IOException, InterruptedException {
      if (inputSplits == null) {
        createInputFormatInstance();
        List<InputSplit> splits =
            inputFormatObj.getSplits(Job.getInstance(conf.getHadoopConfiguration()));
        if (splits == null) {
          throw new IOException(
              HadoopInputFormatIOConstants.COMPUTESPLITS_NULL_GETSPLITS_ERROR_MSG);
        }
        if (splits.isEmpty()) {
          throw new IOException(HadoopInputFormatIOConstants.COMPUTESPLITS_EMPTY_SPLITS_ERROR_MSG);
        }
        boundedSourceEstimatedSize = 0;
        inputSplits = new ArrayList<SerializableSplit>();
        for (InputSplit inputSplit : splits) {
          if (inputSplit == null) {
            throw new IOException(HadoopInputFormatIOConstants.COMPUTESPLITS_NULL_SPLIT_ERROR_MSG);
          }
          boundedSourceEstimatedSize += inputSplit.getLength();
          inputSplits.add(new SerializableSplit(inputSplit));
>>>>>>> Resolved most of the code review comments.
        }
        validateUserInputForKeyAndValue();
      }
    }

    /**
     * Creates instance of InputFormat class provided in class.
     */
    private void createInputFormatInstance() throws IOException, InterruptedException {
      if (inputFormatObj == null) {
        try {
          taskAttemptContext =
              new TaskAttemptContextImpl(conf.getHadoopConfiguration(), new TaskAttemptID());
          inputFormatObj =
              (InputFormat<?, ?>) conf
                  .getHadoopConfiguration()
                  .getClassByName(
                      conf.getHadoopConfiguration().get(
                          HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME)).newInstance();
          /*
           * If InputFormat explicitly implements interface {@link Configurable}, then setConf()
           * method of {@link Configurable} needs to be explicitly called to set all the
           * configuration parameters. For example: InputFormat classes which implement Configurable
           * are {@link org.apache.hadoop.mapreduce.lib.db.DBInputFormat DBInputFormat}, {@link
           * org.apache.hadoop.hbase.mapreduce.TableInputFormat TableInputFormat}, etc.
           */
          if (Configurable.class.isAssignableFrom(inputFormatObj.getClass())) {
            ((Configurable) inputFormatObj).setConf(conf.getHadoopConfiguration());
          }
        } catch (InstantiationException e) {
          throw new IOException("Unable to create InputFormat object: ", e);
        } catch (IllegalAccessException e) {
          throw new IOException("Unable to create InputFormat object: ", e);
        } catch (ClassNotFoundException e) {
          throw new IOException("Unable to create InputFormat object: ", e);
        }
      }
    }
<<<<<<< HEAD
<<<<<<< HEAD

>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
=======
    
=======

>>>>>>> Added synchronization for RecordReader in HadoopInputFormatIO
    /**
     * Throws exception if you set different InputFormat key or value class than InputFormat's
     * actual key or value class. If you set incorrect classes then, it may result in an error like
     * "unexpected extra bytes after decoding" while the decoding process happens. Hence this
     * validation is required.
     */
    private void validateUserInputForKeyAndValue() throws IOException, InterruptedException {
      ParameterizedType genericClassType = determineGenericType();
      boolean isCorrectKeyClassSet = validateClasses(
          genericClassType.getActualTypeArguments()[0].getTypeName(),
          keyCoder,
          "key.class");
      boolean isCorrectValueClassSet = validateClasses(
          genericClassType.getActualTypeArguments()[1].getTypeName(),
          valueCoder,
          "value.class");
      if (!isCorrectKeyClassSet) {
        Class<?> actualClass = conf.getHadoopConfiguration().getClass("key.class", Object.class);
        throw new IllegalArgumentException(String.format(
            HadoopInputFormatIOConstants.WRONG_INPUTFORMAT_KEY_CLASS_ERROR_MSG,
            getExpectedKeyClass().getName(),
            actualClass.getName()));
      }
      if (!isCorrectValueClassSet) {
        Class<?> actualClass = conf.getHadoopConfiguration().getClass("value.class", Object.class);
        throw new IllegalArgumentException(String.format(
            HadoopInputFormatIOConstants.WRONG_INPUTFORMAT_VALUE_CLASS_ERROR_MSG,
            getExpectedValueClass().getName(),
            actualClass.getName()));
      }
    }

    /**
     * Returns true if key/value class set by the user is compatible with the key/value class of a
     * pair returned by RecordReader. User provided key and value classes are validated against
     * parameterized type of InputFormat. If parameterized type has any type parameter such as T,
     * K, V, etc then validation is done by encoding and decoding first pair returned by
     * RecordReader.
     */
    private <T> boolean validateClasses(
        String inputFormatGenericType,
        Coder<T> coder,
        String property) throws IOException, InterruptedException {
      Class<?> inputClass = null;
      try {
        inputClass = Class.forName(inputFormatGenericType);
      } catch (Exception e) {
        /*
         * Given inputFormatGenericType is a type parameter i.e. T, K, V, etc. In such cases class
         * validation for user provided input key/value will not work correctly. Therefore the need
         * to validate key/value classes by encoding and decoding key/value object with the given
         * coder.
         */
        return validateClassUsingCoder(property, coder);
      }
      /*
       * Validates key/value class with InputFormat's parameterized type.
       */
      if (inputClass != null) {
        return validateClassesEquality(inputClass, property);
      }
      return true;
    }

    /**
     * Returns true if first record's key/value encodes and decodes successfully. Also sets
     * expectedKeyClass/expectedValueClass if cloning of key/value fails.
     */
    private <T> boolean validateClassUsingCoder(String property, Coder<T> coder)
        throws IOException, InterruptedException {
      final RecordReader<?, ?> reader;
      reader = fetchFirstRecord();
      if (property.contains("key")) {
        boolean isKeyClassValide = checkEncodingAndDecoding(coder, (T) reader.getCurrentKey());
        if (!isKeyClassValide) {
          this.setExpectedKeyClass(reader.getCurrentKey().getClass());
        }
        return isKeyClassValide;
      } else {
        boolean isValueClassValide = checkEncodingAndDecoding(coder, (T) reader.getCurrentValue());
        if (!isValueClassValide) {
          this.setExpectedValueClass(reader.getCurrentValue().getClass());
        }
        return isValueClassValide;
      }
    }

    /**
     * Returns true if the class set in a given property by the pipeline user is type of given
     * expectedClass.
     */
    private boolean validateClassesEquality(Class<?> expectedClass, String property) {
      Class<?> actualClass = conf.getHadoopConfiguration().getClass(property, Object.class);
      if (!actualClass.isAssignableFrom(expectedClass)) {
        if (property.contains("key")) {
          this.setExpectedKeyClass(expectedClass);
        } else {
          this.setExpectedValueClass(expectedClass);
        }
        return false;
      }
      return true;
    }

    /**
     * Returns false if exception occurs while encoding and decoding the given value with the given
     * coder.
     */
    private <T> boolean checkEncodingAndDecoding(Coder<T> coder, T value) {
      try {
        CoderUtils.clone(coder, value);
      } catch (Exception e) {
        return false;
      }
      return true;
    }

    /**
     * Returns parameterized type of the InputFormat class.
     */
    private ParameterizedType determineGenericType() {
      Class<?> inputFormatClass = inputFormatObj.getClass();
      Type genericSuperclass = null;
      for (;;) {
        genericSuperclass = inputFormatClass.getGenericSuperclass();
        if (genericSuperclass instanceof ParameterizedType)
          break;
        inputFormatClass = inputFormatClass.getSuperclass();
      }
      return (ParameterizedType) genericSuperclass;
    }

    /**
     * Returns RecordReader object of the first split to read first record for validating key/value
     * classes.
     */
    private RecordReader fetchFirstRecord() throws IOException, InterruptedException {
      RecordReader<?, ?> reader =
          inputFormatObj.createRecordReader(inputSplits.get(0).getSplit(), taskAttemptContext);
      if (reader == null) {
        throw new IOException(
            String.format(HadoopInputFormatIOConstants.NULL_CREATE_RECORDREADER_ERROR_MSG,
                inputFormatObj.getClass()));
      }
      reader.initialize(inputSplits.get(0).getSplit(), taskAttemptContext);
      // First record is read to get the InputFormat's key and value classes.
      reader.nextKeyValue();
      return reader;
    }

    @VisibleForTesting
    InputFormat<?, ?> getInputFormat(){
      return inputFormatObj;
    }
<<<<<<< HEAD
    
>>>>>>> Added getInputFormat() in HadoopInputFormatBoundedSource and modified test testReadingWithConfigurableInputFormat.
=======

<<<<<<< HEAD
>>>>>>> Added synchronization for RecordReader in HadoopInputFormatIO
=======
    @VisibleForTesting
    void setInputFormatObj(InputFormat<?, ?> inputFormatObj) {
      this.inputFormatObj = inputFormatObj;
    }

>>>>>>> Mockito commit continued
    @Override
    public Coder<KV<K, V>> getDefaultOutputCoder() {
      return KvCoder.of(keyCoder, valueCoder);
    }

    private Class<?> getExpectedKeyClass() {
      return expectedKeyClass;
    }

    private void setExpectedKeyClass(Class<?> expectedKeyClass) {
      this.expectedKeyClass = expectedKeyClass;
    }

    private Class<?> getExpectedValueClass() {
      return expectedValueClass;
    }

    private void setExpectedValueClass(Class<?> expectedValueClass) {
      this.expectedValueClass = expectedValueClass;
    }

    @Override
    public BoundedReader<KV<K, V>> createReader(PipelineOptions options) throws IOException {
      this.validate();
      if (inputSplit == null) {
<<<<<<< HEAD
<<<<<<< HEAD
        if (!isSourceSplit()) {
          throw new IOException(HadoopInputFormatIOContants.CREATEREADER_UNSPLIT_SOURCE_ERROR_MSG);
        } else {
          throw new IOException(HadoopInputFormatIOContants.CREATEREADER_NULL_SPLIT_ERROR_MSG);
        }
      } else {
        return new HadoopInputFormatReader<Object>(this, keyTranslationFunction,
            valueTranslationFunction, inputSplit.getSplit());
=======
          throw new IOException(HadoopInputFormatIOContants.CREATEREADER_UNSPLIT_SOURCE_ERROR_MSG);
=======
          throw new IOException(HadoopInputFormatIOConstants.CREATEREADER_UNSPLIT_SOURCE_ERROR_MSG);
>>>>>>> Changes for spaces, Constants file name and comments as per Stephens code review comments
      } else {
<<<<<<< HEAD
        return new HadoopInputFormatReader<Object>(this, keyTranslationFunction,
            valueTranslationFunction, inputFormatObj, inputSplit.getSplit());
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
=======
        return new HadoopInputFormatReader<>(
            this,
            keyTranslationFunction,
            valueTranslationFunction,
            inputFormatObj,
            inputSplit.getSplit(),
            taskAttemptContext);
>>>>>>> Resolved most of the code review comments.
      }
    }

    /**
     * BoundedReader for Hadoop InputFormat source.
     *
     * @param <K> Type of keys RecordReader emits.
     * @param <V> Type of values RecordReader emits.
     */
    class HadoopInputFormatReader<K1, V1> extends BoundedSource.BoundedReader<KV<K, V>> {

      private final HadoopInputFormatBoundedSource<K, V> source;
      @Nullable private final SimpleFunction<K1, K> keyTranslationFunction;
      @Nullable private final SimpleFunction<V1, V> valueTranslationFunction;
      private final InputFormat<?, ?> inputFormatObj;
      private final InputSplit split;
      private final TaskAttemptContext taskAttemptContext;
      private RecordReader<K1, V1> currentReader;
      private volatile boolean doneReading = false;
      private long recordsReturned = 0L;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD

      public HadoopInputFormatReader(HadoopInputFormatBoundedSource<K, V> source,
          @Nullable SimpleFunction keyTranslationFunction,
          @Nullable SimpleFunction valueTranslationFunction, InputSplit split) {
        this.source = source;
        this.split = split;
        this.keyTranslationFunction = keyTranslationFunction;
        this.valueTranslationFunction = valueTranslationFunction;
=======
      InputFormat<?, ?> inputFormatObj;
=======
      private InputFormat<?, ?> inputFormatObj;
<<<<<<< HEAD
>>>>>>> Added getInputFormat() in HadoopInputFormatBoundedSource and modified test testReadingWithConfigurableInputFormat.
=======
      private RecordReader<T, T> currentReader;
      private KV<K, V> currentRecord;
>>>>>>> Changes for spaces, Constants file name and comments as per Stephens code review comments
=======
>>>>>>> Resolved most of the code review comments.
=======
      // Variable added to track the progress of the RecordReader.
      private AtomicDouble progressValue = new AtomicDouble();
>>>>>>> Added AtomicDouble to track the progress of the RecordReader

      private HadoopInputFormatReader(HadoopInputFormatBoundedSource<K, V> source,
          @Nullable SimpleFunction keyTranslationFunction,
          @Nullable SimpleFunction valueTranslationFunction,
          InputFormat<?,?> inputFormatObj,
          InputSplit split,
          TaskAttemptContext taskAttemptContext) {
        this.source = source;
        this.keyTranslationFunction = keyTranslationFunction;
        this.valueTranslationFunction = valueTranslationFunction;
        this.inputFormatObj = inputFormatObj;
        this.split = split;
<<<<<<< HEAD
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
=======
        this.taskAttemptContext = taskAttemptContext;
>>>>>>> Resolved most of the code review comments.
      }

      @Override
      public HadoopInputFormatBoundedSource<K, V> getCurrentSource() {
        return source;
      }

      @Override
      public boolean start() throws IOException {
        try {
<<<<<<< HEAD
          TaskAttemptContextImpl attemptContext = new TaskAttemptContextImpl(
              source.getConfiguration().getHadoopConfiguration(), new TaskAttemptID());
<<<<<<< HEAD
          Job job = Job.getInstance(source.getConfiguration().getHadoopConfiguration());
          InputFormat<?, ?> inputFormatObj =
              (InputFormat<?, ?>) job.getInputFormatClass().newInstance();
=======
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
=======
          recordsReturned = 0;
>>>>>>> Resolved most of the code review comments.
          currentReader =
              (RecordReader<K1, V1>) inputFormatObj.createRecordReader(split, taskAttemptContext);
          if (currentReader != null) {
            /*
             * CurrentReader object could be accessed concurrently by multiple sources. Hence, to be
             * on safer side, it has been added in synchronized block.
             */
            synchronized (currentReader) {
              currentReader.initialize(split, taskAttemptContext);
              if (currentReader.nextKeyValue()) {
                recordsReturned++;
                return true;
              }
            }
          } else {
            throw new IOException(String.format(
                HadoopInputFormatIOConstants.NULL_CREATE_RECORDREADER_ERROR_MSG,
                inputFormatObj.getClass()));
          }
          synchronized (currentReader) {
            currentReader = null;
          }
        } catch (InterruptedException e) {
<<<<<<< HEAD
<<<<<<< HEAD
          throw new IOException("Unable to read data : ",e);
        } catch (InstantiationException e) {
          throw new IOException("Unable to create InputFormat : ",e);
        } catch (IllegalAccessException e) {
          throw new IOException("Unable to create InputFormat : ",e);
        } catch (ClassNotFoundException e) {
          throw new IOException("Unable to create InputFormat : ",e);
=======
          throw new IOException("Unable to read data : ", e);
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
=======
          throw new IOException("Unable to read data: ", e);
>>>>>>> Changes for spaces, Constants file name and comments as per Stephens code review comments
        }
        doneReading = true;
        return false;
      }

      @Override
      public boolean advance() throws IOException {
        try {
          synchronized (currentReader) {
            progressValue = new AtomicDouble(getProgress());
            if (currentReader != null && currentReader.nextKeyValue()) {
              recordsReturned++;
              return true;
            }
          }
          doneReading = true;
        } catch (InterruptedException e) {
<<<<<<< HEAD
<<<<<<< HEAD
          throw new IOException("Unable to read data : ",e);
=======
          throw new IOException("Unable to read data : ", e);
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
=======
          throw new IOException("Unable to read data: ", e);
>>>>>>> Changes for spaces, Constants file name and comments as per Stephens code review comments
        }
        return false;
      }

<<<<<<< HEAD
      /**
       * Returns the pair of key and value of the next record in sequence.
       */
      public KV<K, V> nextPair() throws IOException, InterruptedException {
        // Transform key if required.
<<<<<<< HEAD
        K key = transformKeyOrValue(currentReader.getCurrentKey(), keyTranslationFunction, keyCoder);
=======
        K key =
            transformKeyOrValue(currentReader.getCurrentKey(), keyTranslationFunction, keyCoder);
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
        // Transform value if required.
        V value = transformKeyOrValue(currentReader.getCurrentValue(), valueTranslationFunction,
            valueCoder);
        recordsReturned++;
=======
      @Override
      public KV<K, V> getCurrent() throws NoSuchElementException {
        K key = null;
        V value = null;
        try {
          // Transform key if translation function is provided.
          key =
              transformKeyOrValue((K1) currentReader.getCurrentKey(), keyTranslationFunction,
                  keyCoder);
          // Transform value if if translation function is provided.
          value =
              transformKeyOrValue((V1) currentReader.getCurrentValue(), valueTranslationFunction,
                  valueCoder);
        } catch (IOException | InterruptedException e) {
          LOG.error(HadoopInputFormatIOConstants.GET_CURRENT_ERROR_MSG + e);
          return null;
        }
        if (key == null) {
          throw new NoSuchElementException();
        }
>>>>>>> Resolved most of the code review comments.
        return KV.of(key, value);
      }

      /**
       * Returns the serialized output of transformed key or value object.
       * @throws ClassCastException
       * @throws CoderException
       */
      private <T, T1> T1 transformKeyOrValue(T input,
          @Nullable SimpleFunction<T, T1> simpleFunction, Coder<T1> coder) throws CoderException,
          ClassCastException {
        T1 output;
        if (null != simpleFunction) {
          output = simpleFunction.apply(input);
        } else {
          output = (T1) input;
        }
        return cloneIfPossiblyMutable((T1) output, coder);
      }

      /**
       * Many objects used by Beam are mutable, but the Hadoop InputFormats tend to re-use the same
       * object when returning them. Hence mutable objects are cloned.
       */
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
      private <T1 extends Object> T1 clone(T1 input, Coder<T1> coder)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
          throws IOException, InterruptedException, CoderException, ClassCastException{
=======
          throws IOException, InterruptedException, CoderException,ClassCastException {
>>>>>>> Javadoc changes
        // If the input object is not of known immutable type, clone the object.
          if (!isKnownImmutable(input)) {
            input = CoderUtils.clone(coder, input);
          }
<<<<<<< HEAD
       
=======
        
>>>>>>> Javadoc changes
=======

=======
>>>>>>> Added getInputFormat() in HadoopInputFormatBoundedSource and modified test testReadingWithConfigurableInputFormat.
          throws IOException, InterruptedException, CoderException, ClassCastException {
=======
      private <T extends Object> T cloneIfPossiblyMutable(T input, Coder<T> coder)
=======
      private <T> T cloneIfPossiblyMutable(T input, Coder<T> coder)
>>>>>>> Added data validation in HIFIOWithPostgresIT.java
          throws CoderException, ClassCastException {
>>>>>>> Resolved most of the code review comments.
=======
      private <T> T cloneIfPossiblyMutable(T input, Coder<T> coder) throws CoderException,
          ClassCastException {
>>>>>>> Changes as per code review by Dipti, revisited code
        // If the input object is not of known immutable type, clone the object.
        if (!isKnownImmutable(input)) {
          input = CoderUtils.clone(coder, input);
        }
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
        return input;
      }

      /**
       * Utility method to check if the passed object is of a known immutable type.
       */
      private boolean isKnownImmutable(Object o) {
        Set<Class<?>> immutableTypes = new HashSet<Class<?>>(
            Arrays.asList(
                String.class,
                Byte.class,
                Short.class,
                Integer.class,
                Long.class,
                Float.class,
                Double.class,
                Boolean.class,
                BigInteger.class,
                BigDecimal.class));
        return immutableTypes.contains(o.getClass());
      }

      @Override
      public void close() throws IOException {
        LOG.info("Closing reader after reading {} records.", recordsReturned);
        if (currentReader != null) {
          currentReader.close();
          currentReader = null;
        }
      }

      @Override
      public Double getFractionConsumed() {
        if (doneReading) {
          return 1.0;
        }
        if (currentReader == null || recordsReturned == 0) {
          return 0.0;
        }
        return progressValue.doubleValue();
      }

      /**
       * Returns RecordReader's progress.
       */
      private Double getProgress() {
        try {
          synchronized (currentReader) {
            return (double) currentReader.getProgress();
          }
        } catch (IOException | InterruptedException e) {
          LOG.error(HadoopInputFormatIOConstants.GETFRACTIONSCONSUMED_ERROR_MSG + e.getMessage(), e);
          return 0.0;
        }
      }

      @Override
      public final long getSplitPointsRemaining() {
        if (doneReading) {
          return 0;
        }
        /**
         * This source does not currently support dynamic work rebalancing, so remaining parallelism
         * is always 1.
         */
        return 1;
      }
    }

    /**
     * A wrapper to allow Hadoop {@link org.apache.hadoop.mapreduce.InputSplit} to be serialized
     * using Java's standard serialization mechanisms. Note that the InputSplit has to be Writable.
     */
    public static class SerializableSplit implements Externalizable {

      private InputSplit split;

      public SerializableSplit() {}

      public SerializableSplit(InputSplit split) {
        checkArgument(split instanceof Writable, String
            .format(HadoopInputFormatIOConstants.SERIALIZABLE_SPLIT_WRITABLE_ERROR_MSG, split));
        this.split = split;
      }

      public InputSplit getSplit() {
        return split;
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(split.getClass().getCanonicalName());
        ((Writable) split).write(out);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        String className = in.readUTF();
        try {
          split = (InputSplit) Class.forName(className).newInstance();
          ((Writable) split).readFields(in);
        } catch (InstantiationException | IllegalAccessException e) {
<<<<<<< HEAD
<<<<<<< HEAD
          throw new IOException("Unable to create split : "+e);
=======
          throw new IOException("Unable to create split : " + e);
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
=======
          throw new IOException("Unable to create split: " + e);
>>>>>>> Changes for spaces, Constants file name and comments as per Stephens code review comments
        }
      }
    }

    public boolean producesSortedKeys(PipelineOptions arg0) throws Exception {
      return false;
    }

  }

  /**
   * A wrapper to allow Hadoop {@link org.apache.hadoop.conf.Configuration} to be serialized using
   * Java's standard serialization mechanisms. Note that the org.apache.hadoop.conf.Configuration
   * is Writable.
   */
  public static class SerializableConfiguration implements Externalizable {

    private Configuration conf;

    public SerializableConfiguration() {}

    public SerializableConfiguration(Configuration conf) {
      this.conf = conf;
    }

    public Configuration getHadoopConfiguration() {
      return conf;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeUTF(conf.getClass().getCanonicalName());
      ((Writable) conf).write(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      String className = in.readUTF();
      try {
        conf = (Configuration) Class.forName(className).newInstance();
        conf.readFields(in);
      } catch (InstantiationException | IllegalAccessException e) {
<<<<<<< HEAD
<<<<<<< HEAD
        throw new IOException("Unable to create configuration : "+e);
      }
    }
  }
=======
	private static final Logger LOG = LoggerFactory.getLogger(HadoopInputFormatIO.class);

	/**
	 * Creates an uninitialized HadoopInputFormatIO.Read. Before use, the {@code Read} must be
	 * initialized with a HadoopInputFormatIO.Read#withConfiguration(HadoopConfiguration) that
	 * specifies the source. A key/value translation may also optionally be specified using
	 * HadoopInputFormatIO.Read#withKeyTranslation/HadoopInputFormatIO.Read#withValueTranslation.
	 */
	public static <K, V> Read<K, V> read() {
		return new AutoValue_HadoopInputFormatIO_Read.Builder<K, V>().build();
	}

	/**
	 * A {@link PTransform} that reads from for any data source which implements Hadoop InputFormat.
	 * For e.g. Cassandra, Elasticsearch, HBase, Redis, Postgres etc. See the class-level Javadoc on
	 * {@link HadoopInputFormatIO} for more information.
	 * 
	 * @param <K> Type of keys to be read.
	 * @param <V> Type of values to be read.
	 * @see HadoopInputFormatIO
	 */
	@AutoValue
	public abstract static class Read<K, V> extends PTransform<PBegin, PCollection<KV<K, V>>> {

		public static TypeDescriptor<?> inputFormatClass;
		public static TypeDescriptor<?> inputFormatKeyClass;
		public static TypeDescriptor<?> inputFormatValueClass;

		/**
		 * Returns the Hadoop Configuration which contains specification of source.
		 */
		@Nullable
		public abstract SerializableConfiguration getConfiguration();

		@Nullable
		public abstract SimpleFunction<?, K> getKeyTranslationFunction();

		@Nullable
		public abstract SimpleFunction<?, V> getValueTranslationFunction();

		@Nullable
		public abstract TypeDescriptor<K> getKeyClass();

		@Nullable
		public abstract TypeDescriptor<V> getValueClass();

		abstract Builder<K, V> toBuilder();

		@AutoValue.Builder
		abstract static class Builder<K, V> {
			abstract Builder<K, V> setConfiguration(SerializableConfiguration configuration);

			abstract Builder<K, V> setKeyTranslationFunction(SimpleFunction<?, K> function);

			abstract Builder<K, V> setValueTranslationFunction(SimpleFunction<?, V> function);

			abstract Builder<K, V> setKeyClass(TypeDescriptor<K> keyClass);

			abstract Builder<K, V> setValueClass(TypeDescriptor<V> valueClass);

			abstract Read<K, V> build();
		}

		/**
		 * Returns a new {@link HadoopInputFormatIO.Read} that will read from the source using the 
		 * options indicated by the given configuration.
		 *
		 * <p>
		 * Does not modify this object.
		 */
		public Read<K, V> withConfiguration(Configuration configuration) {
			validateConfiguration(configuration);
			inputFormatClass =
					TypeDescriptor.of(configuration.getClass(HadoopInputFormatIOContants.INPUTFORMAT_CLASSNAME, null));
			inputFormatKeyClass = TypeDescriptor.of(configuration.getClass(HadoopInputFormatIOContants.KEY_CLASS, null));
			inputFormatValueClass = TypeDescriptor.of(configuration.getClass(HadoopInputFormatIOContants.VALUE_CLASS, null));
			// Sets the configuration.
			Builder<K, V> builder =
					toBuilder().setConfiguration(new SerializableConfiguration(configuration));
			// Sets the output key class to InputFormat key class if withKeyTranslation() is not called
			// yet.
			if (this.getKeyClass() == null) {
				builder.setKeyClass((TypeDescriptor<K>) inputFormatKeyClass);
			}
			// Sets the output value class to InputFormat value class if withValueTranslation() is not
			// called yet.
			if (this.getValueClass() == null) {
				builder.setValueClass((TypeDescriptor<V>) inputFormatValueClass);
			}
			return builder.build();
		}

		/**
		 * Validates that the mandatory configuration properties such as InputFormat class, InputFormat
		 * key class and InputFormat value class are provided in the given Hadoop configuration.
		 */
		private void validateConfiguration(Configuration configuration) {
			checkNotNull(configuration, HadoopInputFormatIOContants.NULL_CONFIGURATION_ERROR_MSG);
			checkNotNull(configuration.get("mapreduce.job.inputformat.class"), HadoopInputFormatIOContants.MISSING_INPUTFORMAT_ERROR_MSG);
			checkNotNull(configuration.get("key.class"),
					HadoopInputFormatIOContants.MISSING_INPUTFORMAT_KEY_CLASS_ERROR_MSG);
			checkNotNull(configuration.get("value.class"),
					HadoopInputFormatIOContants.MISSING_INPUTFORMAT_VALUE_CLASS_ERROR_MSG);
		}

		/**
		 * Returns a new {@link HadoopInputFormatIO.Read} that will transform the keys read from the
		 * source using the given key translation function.
		 *
		 * <p>
		 * Does not modify this object.
		 */
		public Read<K, V> withKeyTranslation(SimpleFunction<?, K> function) {
			checkNotNull(function, HadoopInputFormatIOContants.NULL_KEY_TRANSLATIONFUNC_ERROR_MSG);
			// Sets key class to key translation function's output class type.
			return toBuilder().setKeyTranslationFunction(function)
					.setKeyClass((TypeDescriptor<K>) function.getOutputTypeDescriptor()).build();
		}

		/**
		 * Returns a new {@link HadoopInputFormatIO.Read} that will transform the values read from the
		 * source using the given value translation function.
		 *
		 * <p>
		 * Does not modify this object.
		 */
		public Read<K, V> withValueTranslation(SimpleFunction<?, V> function) {
			checkNotNull(function, HadoopInputFormatIOContants.NULL_VALUE_TRANSLATIONFUNC_ERROR_MSG);
			// Sets value class to value translation function's output class type.
			return toBuilder().setValueTranslationFunction(function)
					.setValueClass((TypeDescriptor<V>) function.getOutputTypeDescriptor()).build();
		}

		@Override
		public PCollection<KV<K, V>> expand(PBegin input) {
			HadoopInputFormatBoundedSource<K, V> source = new HadoopInputFormatBoundedSource<K, V>(
					this.getConfiguration(), this.getKeyCoder(), this.getValueCoder(),
					this.getKeyTranslationFunction(), this.getValueTranslationFunction(), null);
			return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(source));
		}

		/**
		 * Validates inputs provided by the pipeline user before reading the data.
		 */
		@Override
		public void validate(PBegin input) {
			checkNotNull(this.getConfiguration(),
					HadoopInputFormatIOContants.MISSING_CONFIGURATION_ERROR_MSG);
			// Validate that the key translation input type must be same as key class of InputFormat.
			validateTranslationFunction(inputFormatKeyClass, getKeyTranslationFunction(),
					HadoopInputFormatIOContants.WRONG_KEY_TRANSLATIONFUNC_ERROR_MSG);
			// Validate that the value translation input type must be same as value class of InputFormat.
			validateTranslationFunction(inputFormatValueClass, getValueTranslationFunction(),
					HadoopInputFormatIOContants.WRONG_VALUE_TRANSLATIONFUNC_ERROR_MSG);
			getKeyAndValueCoder(input);
		}


		/**
		 * Validates translation function given for key/value translation.
		 */
		private void validateTranslationFunction(TypeDescriptor<?> inputType,
				SimpleFunction<?, ?> simpleFunction, String errorMsg) {
			if (simpleFunction != null) {
				if (!simpleFunction.getInputTypeDescriptor().equals(inputType)) {
					throw new IllegalArgumentException(
							String.format(errorMsg, inputFormatClass.getRawType(), inputType.getRawType()));
				}
			}
		}

		private Coder<K> keyCoder;
		private Coder<V> valueCoder;

		/** Sets the key and value coder based on the key class and value class. */
		protected void getKeyAndValueCoder(PBegin input) {
			CoderRegistry coderRegistry = input.getPipeline().getCoderRegistry();
			keyCoder = getDefaultCoder(getKeyClass(), coderRegistry);
			valueCoder = getDefaultCoder(getValueClass(), coderRegistry);
		}

		/**
		 * Returns the default coder for a given type descriptor. If type descriptor class is of type
		 * Writable, then WritableCoder is returned, else CoderRegistry is queried for the correct
		 * coder.
		 */
		public <T> Coder<T> getDefaultCoder(TypeDescriptor<?> typeDesc, CoderRegistry coderRegistry) {
			Class classType = typeDesc.getRawType();
			if (Writable.class.isAssignableFrom(classType)) {
				return (Coder<T>) WritableCoder.of(classType);
			} else {
				try {
					return (Coder<T>) coderRegistry.getCoder(typeDesc);
				} catch (CannotProvideCoderException e) {
					throw new IllegalStateException(
							String.format(HadoopInputFormatIOContants.CANNOT_FIND_CODER_ERROR_MSG, typeDesc)
							+ e.getMessage(),
							e);
				}
			}
		}

		public Coder<K> getKeyCoder() {
			return keyCoder;
		}

		public Coder<V> getValueCoder() {
			return valueCoder;
		}

		@Override
		public void populateDisplayData(DisplayData.Builder builder) {
			super.populateDisplayData(builder);
			if (this.getConfiguration().getHadoopConfiguration() != null) {
				Iterator<Entry<String, String>> configProperties =
						this.getConfiguration().getHadoopConfiguration().iterator();
				while (configProperties.hasNext()) {
					Entry<String, String> property = configProperties.next();
					builder.add(DisplayData.item(property.getKey(), property.getValue())
							.withLabel(property.getKey()));
				}
			}
			builder
			.addIfNotNull(DisplayData.item("KeyClass", getKeyClass().getRawType())
					.withLabel("Output key class"))
					.addIfNotNull(DisplayData.item("ValueClass", getValueClass().getRawType())
							.withLabel("Output value class"));
			if (getKeyTranslationFunction() != null)
				builder.addIfNotNull(
						DisplayData.item("KeyTranslationSimpleFunction", getKeyTranslationFunction().toString())
						.withLabel("Key translation SimpleFunction"));
			if (getValueTranslationFunction() != null)
				builder.addIfNotNull(DisplayData
						.item("ValueTranslationSimpleFunction", getValueTranslationFunction().toString())
						.withLabel("Value translation SimpleFunction"));

		}
	}

	/**
	 * Bounded source implementation for HadoopInputFormatIO
	 * 
	 * @param <K> Type of keys to be read.
	 * @param <V> Type of values to be read.
	 */
	public static class HadoopInputFormatBoundedSource<K, V> extends BoundedSource<KV<K, V>>
	implements Serializable {
		protected final SerializableConfiguration conf;
		protected final Coder<K> keyCoder;
		protected final Coder<V> valueCoder;
		protected final SimpleFunction<?, K> keyTranslationFunction;
		protected final SimpleFunction<?, V> valueTranslationFunction;
		protected final SerializableSplit inputSplit;
		private transient List<SerializableSplit> inputSplits;
		private long boundedSourceEstimatedSize = 0;
		

		public HadoopInputFormatBoundedSource(SerializableConfiguration conf, Coder<K> keyCoder,
				Coder<V> valueCoder) {
			this(conf, keyCoder, valueCoder, null, null, null);
		}

		public HadoopInputFormatBoundedSource(SerializableConfiguration conf, Coder<K> keyCoder,
				Coder<V> valueCoder, SimpleFunction<?, K> keyTranslationFunction,
				SimpleFunction<?, V> valueTranslationFunction, SerializableSplit inputSplit) {
			this.conf = conf;
			this.inputSplit = inputSplit;
			this.keyCoder = keyCoder;
			this.valueCoder = valueCoder;
			this.keyTranslationFunction = keyTranslationFunction;
			this.valueTranslationFunction = valueTranslationFunction;
		}

		public SerializableConfiguration getConfiguration() {
			return conf;
		}

		@Override
		public void validate() {
			checkNotNull(conf, HadoopInputFormatIOContants.MISSING_CONFIGURATION_SOURCE_ERROR_MSG);
			checkNotNull(keyCoder, HadoopInputFormatIOContants.MISSING_KEY_CODER_SOURCE_ERROR_MSG);
			checkNotNull(valueCoder, HadoopInputFormatIOContants.MISSING_VALUE_CODER_SOURCE_ERROR_MSG);
		}

		// Indicates if the source is split or not.
		private transient boolean isSourceSplit = false;

		public boolean isSourceSplit() {
			return isSourceSplit;
		}

		public void setSourceSplit(boolean isSourceSplit) {
			this.isSourceSplit = isSourceSplit;
		}

		@Override
		public List<BoundedSource<KV<K, V>>> splitIntoBundles(long desiredBundleSizeBytes,
				PipelineOptions options) throws Exception {
			if (inputSplit == null) {
				if (inputSplits == null) {
					computeSplits();
				}
				LOG.info("Generated {} splits each of size {} ", inputSplits.size(),
						inputSplits.get(0).getSplit().getLength());
				setSourceSplit(true);
				return Lists.transform(inputSplits,
						new Function<SerializableSplit, BoundedSource<KV<K, V>>>() {
					@Override
					public BoundedSource<KV<K, V>> apply(SerializableSplit serializableInputSplit) {
						HadoopInputFormatBoundedSource<K, V> hifBoundedSource =
								new HadoopInputFormatBoundedSource<K, V>(conf, keyCoder, valueCoder,
										keyTranslationFunction, valueTranslationFunction, serializableInputSplit);
						return hifBoundedSource;
					}
				});
			} else {
				setSourceSplit(true);
				LOG.info("Not splitting source {} because source is already split.", this);
				return ImmutableList.of((BoundedSource<KV<K, V>>) this);
			}
		}

		@Override
		public long getEstimatedSizeBytes(PipelineOptions po) throws Exception {
			if (inputSplits == null) {
				computeSplits();
			}
			return boundedSourceEstimatedSize;
		}


		/**
		 * This is helper function to compute splits. This method will also calculate size of the data
		 * being read. Note : This method is called exactly once, the splits are retrieved and cached
		 * for further use by splitIntoBundles() and getEstimatesSizeBytes().
		 */
		private void computeSplits() throws IOException, IllegalAccessException, InstantiationException,
		InterruptedException, ClassNotFoundException {
			Job job = Job.getInstance(conf.getHadoopConfiguration());
			InputFormat<?, ?> inputFormatObj= job.getInputFormatClass().newInstance();
			/**
			 * Sets configuration if provided InputFormat implements Configurable.
			 */
			if (Configurable.class.isAssignableFrom(inputFormatObj.getClass())) {
				((Configurable) inputFormatObj).setConf(conf.getHadoopConfiguration());
			}
			List<InputSplit> splits=inputFormatObj.getSplits(job);
			if (splits == null) {
				throw new IOException(HadoopInputFormatIOContants.COMPUTESPLITS_NULL_SPLITS_ERROR_MSG);
			}
			if (splits.isEmpty()) {
				throw new IOException(HadoopInputFormatIOContants.COMPUTESPLITS_EMPTY_SPLITS_ERROR_MSG);
			}
			boundedSourceEstimatedSize = 0;
			inputSplits = new ArrayList<SerializableSplit>();
			for (InputSplit inputSplit : splits) {
				boundedSourceEstimatedSize = boundedSourceEstimatedSize + inputSplit.getLength();
				inputSplits.add(new SerializableSplit(inputSplit));
			}
		}

		@Override
		public Coder<KV<K, V>> getDefaultOutputCoder() {
			return KvCoder.of(keyCoder, valueCoder);
		}

		@Override
		public BoundedReader<KV<K, V>> createReader(PipelineOptions options) throws IOException {
			this.validate();
			if (inputSplit == null) {
				if (!isSourceSplit()) {
					throw new IOException(HadoopInputFormatIOContants.CREATEREADER_UNSPLIT_SOURCE_ERROR_MSG);
				} else {
					throw new IOException(HadoopInputFormatIOContants.CREATEREADER_NULL_SPLIT_ERROR_MSG);
				}
			} else {
				return new HadoopInputFormatReader<Object>(this, keyTranslationFunction,
						valueTranslationFunction, inputSplit.getSplit());
			}
		}

		/** BoundedReader for HadoopInputFormatSource. */
		class HadoopInputFormatReader<T extends Object> extends BoundedSource.BoundedReader<KV<K, V>> {

			private final HadoopInputFormatBoundedSource<K, V> source;
			private final InputSplit split;
			@Nullable
			private final SimpleFunction<T, K> keyTranslationFunction;
			@Nullable
			private final SimpleFunction<T, V> valueTranslationFunction;
			private volatile boolean doneReading = false;
			private long recordsReturned = 0L;

			public HadoopInputFormatReader(HadoopInputFormatBoundedSource<K, V> source,
					@Nullable SimpleFunction keyTranslationFunction,
					@Nullable SimpleFunction valueTranslationFunction, InputSplit split) {
				this.source = source;
				this.split = split;
				this.keyTranslationFunction = keyTranslationFunction;
				this.valueTranslationFunction = valueTranslationFunction;
			}

			@Override
			public HadoopInputFormatBoundedSource<K, V> getCurrentSource() {
				return source;
			}

			private RecordReader<T, T> currentReader;
			private KV<K, V> currentRecord;

			@Override
			public boolean start() throws IOException {
				try {
					TaskAttemptContextImpl attemptContext = new TaskAttemptContextImpl(
							source.getConfiguration().getHadoopConfiguration(), new TaskAttemptID());
					Job job = Job.getInstance(source.getConfiguration().getHadoopConfiguration());
					InputFormat<?, ?> inputFormatObj= job.getInputFormatClass().newInstance();
		            /**
		             * Sets configuration if provided InputFormat implements Configurable.
		             */
					if (Configurable.class.isAssignableFrom(inputFormatObj.getClass())) {
						((Configurable) inputFormatObj).setConf(source.getConfiguration().getHadoopConfiguration());
					}

					currentReader =
							(RecordReader<T, T>) inputFormatObj.createRecordReader(split, attemptContext);
					if (currentReader != null) {
						currentReader.initialize(split, attemptContext);
						if (currentReader.nextKeyValue()) {
							currentRecord = nextPair();
							return true;
						}
					} else {
						throw new IOException(
								String.format(HadoopInputFormatIOContants.NULL_CREATE_RECORDREADER_ERROR_MSG,
										inputFormatObj.getClass()));
					}
				} catch (InterruptedException e) {
					throw new IOException("Unable to read data : ",e);
				} catch (InstantiationException e) {
					throw new IOException("Unable to create InputFormat : ",e);
				} catch (IllegalAccessException e) {
					throw new IOException("Unable to create InputFormat : ",e);
				} catch (ClassNotFoundException e) {
					throw new IOException("Unable to create InputFormat : ",e);
				}
				currentReader = null;
				currentRecord = null;
				doneReading = true;
				return false;
			}

			@Override
			public boolean advance() throws IOException {
				try {
					if (currentReader != null && currentReader.nextKeyValue()) {
						currentRecord = nextPair();
						return true;
					}
					currentRecord = null;
					doneReading = true;
				} catch (InterruptedException e) {
					throw new IOException("Unable to read data : ",e);
				}
				return false;
			}

			/**
			 * Returns the pair of key and value of the next record in sequence.
			 */
			public KV<K, V> nextPair() throws IOException, InterruptedException {
				// Transform key if required.
				K key = transformKeyOrValue(currentReader.getCurrentKey(), keyTranslationFunction, keyCoder);
				// Transform value if required.
				V value = transformKeyOrValue(currentReader.getCurrentValue(), valueTranslationFunction,
						valueCoder);
				recordsReturned++;
				return KV.of(key, value);
			}

			/**
			 * Return the serialized output by cloning. Given input is transformed with the given
			 * SimpleFunction.
			 */
			private <T1 extends Object> T1 transformKeyOrValue(T input,
					@Nullable SimpleFunction<T, T1> simpleFunction, Coder<T1> coder)
							throws IOException, InterruptedException {
				T1 output;
				if (null != simpleFunction) {
					output = simpleFunction.apply(input);
				} else {
					output = (T1) input;
				}
				return clone(output, coder);
			}

			/**
			 * Cloning/Serialization is required to take care of Hadoop's mutable objects if returned by
			 * RecordReader as Beam needs immutable objects.
			 */
			private <T1 extends Object> T1 clone(T1 input, Coder<T1> coder)

					throws IOException, InterruptedException, CoderException, ClassCastException{
				// If the input object is not of known immutable type, clone the object.
				if (!isKnownImmutable(input)) {
					input = CoderUtils.clone(coder, input);
				}
				return input;
			}

			/**
			 * Utility method to check if the passed object is of a known immutable type.
			 */
			private boolean isKnownImmutable(Object o) {
				Set<Class<?>> immutableTypes = new HashSet<Class<?>>(
						Arrays.asList(String.class, Byte.class, Short.class, Integer.class, Long.class,
								Float.class, Double.class, Boolean.class, BigInteger.class, BigDecimal.class));
				return immutableTypes.contains(o.getClass());
			}

			@Override
			public KV<K, V> getCurrent() throws NoSuchElementException {
				if (currentRecord == null) {
					throw new NoSuchElementException();
				}
				return currentRecord;
			}

			@Override
			public void close() throws IOException {
				LOG.info("Closing reader after reading {} records.", recordsReturned);
				if (currentReader != null) {
					currentReader.close();
					currentReader = null;
				}
				currentRecord = null;
			}

			@Override
			public Double getFractionConsumed() {
				if (currentReader == null || recordsReturned == 0) {
					return 0.0;
				}
				if (doneReading) {
					return 1.0;
				}
				return getProgress();
			}

			/**
			 * Returns RecordReader's Progress.
			 */
			private Double getProgress() {
				try {
					return (double) currentReader.getProgress();
				} catch (IOException | InterruptedException e) {
					LOG.error(HadoopInputFormatIOContants.GETFRACTIONSCONSUMED_ERROR_MSG + e.getMessage(), e);
					return null;
				}
			}

			@Override
			public final long getSplitPointsRemaining() {
				if (doneReading) {
					return 0;
				}
				/**
				 * This source does not currently support dynamic work rebalancing, so remaining parallelism
				 * is always 1.
				 */
				return 1;
			}
		}

		/**
		 * A wrapper to allow Hadoop {@link org.apache.hadoop.mapreduce.InputSplit}s to be serialized
		 * using Java's standard serialization mechanisms. Note that the InputSplit has to be Writable
		 * (which mostly are).
		 */
		public class SerializableSplit implements Externalizable {

			private static final long serialVersionUID = 0L;

			private InputSplit split;

			public SerializableSplit() {}

			public SerializableSplit(InputSplit split) {
				checkArgument(split instanceof Writable, String
						.format(HadoopInputFormatIOContants.SERIALIZABLE_SPLIT_WRITABLE_ERROR_MSG, split));
				this.split = split;
			}

			public InputSplit getSplit() {
				return split;
			}

			@Override
			public void writeExternal(ObjectOutput out) throws IOException {
				out.writeUTF(split.getClass().getCanonicalName());
				((Writable) split).write(out);
			}

			@Override
			public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				String className = in.readUTF();
				try {
					split = (InputSplit) Class.forName(className).newInstance();
					((Writable) split).readFields(in);
				} catch (InstantiationException | IllegalAccessException e) {
					throw new IOException("Unable to create split : "+e);
				}
			}
		}


		public boolean producesSortedKeys(PipelineOptions arg0) throws Exception {
			return false;
		}

	}

	/**
	 * A wrapper to allow Hadoop {@link org.apache.hadoop.conf.Configuration} to be serialized using
	 * Java's standard serialization mechanisms. Note that the org.apache.hadoop.conf.Configuration
	 * has to be Writable (which mostly are).
	 */
	public static class SerializableConfiguration implements Externalizable {
		private static final long serialVersionUID = 0L;

		private Configuration conf;

		public SerializableConfiguration() {}

		public SerializableConfiguration(Configuration conf) {
			this.conf = conf;
		}

		public Configuration getHadoopConfiguration() {
			return conf;
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeUTF(conf.getClass().getCanonicalName());
			((Writable) conf).write(out);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			String className = in.readUTF();
			try {
				conf = (Configuration) Class.forName(className).newInstance();
				conf.readFields(in);
			} catch (InstantiationException | IllegalAccessException e) {
				throw new IOException("Unable to create configuration : "+e);
			}
		}
	}
>>>>>>> Changes in HadoopInputFormat IO to check if InputFormat implements Configurable interface, need to setConf(conf) in that case
=======
        throw new IOException("Unable to create configuration : " + e);
=======
        throw new IOException("Unable to create configuration: " + e);
>>>>>>> Changes for spaces, Constants file name and comments as per Stephens code review comments
      }
    }
  }
>>>>>>> Modification in HadoopInputFormat and added unit test to test splitIntoBundles if get splits returns split list having null values.
}
