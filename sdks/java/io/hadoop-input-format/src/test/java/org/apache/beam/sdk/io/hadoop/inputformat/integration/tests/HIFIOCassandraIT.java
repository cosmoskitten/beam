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
package org.apache.beam.sdk.io.hadoop.inputformat.integration.tests;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
<<<<<<< HEAD
=======
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIOConstants;
>>>>>>> Changes for spaces, Constants file name and comments as per Stephens code review comments
import org.apache.beam.sdk.io.hadoop.inputformat.custom.options.HIFTestOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.Row;

/**
 * Runs integration test to validate HadoopInputFromatIO for a Cassandra instance.
 *
 * You need to pass Cassandra server IP and port in beamTestPipelineOptions.
 *
 * <p>
<<<<<<< HEAD
 * You can run just this test by doing the following:
 * mvn test-compile compile failsafe:integration-test -D beamTestPipelineOptions='[ "--serverIp=1.2.3.4",
 * "--serverPort=<port>" ]'
=======
 * You can run just this test by doing the following: mvn test-compile compile
 * failsafe:integration-test -D beamTestPipelineOptions='[ "--serverIp=1.2.3.4",
 * "--serverPort=<port>" ]' -Dit.test=HIFIOCassandraIT -DskipITs=false
<<<<<<< HEAD
>>>>>>> Added tests with scientist data
 *
=======
>>>>>>> Review comments followed in Cassandra IT
 */
@RunWith(JUnit4.class)
public class HIFIOCassandraIT implements Serializable {

<<<<<<< HEAD
  private static final Logger LOGGER = LoggerFactory.getLogger(HIFIOCassandraIT.class);
<<<<<<< HEAD
  private static final String CASSANDRA_KEYSPACE = "ycsb";
  private static final String CASSANDRA_TABLE = "usertable";
  private static Configuration conf;
=======
  private static final String CASSANDRA_KEYSPACE = "beamdb";
  private static final String CASSANDRA_TABLE = "scientists";
>>>>>>> Elastic, Cassandra embedded code and ITs
=======
  private static final String CASSANDRA_KEYSPACE = "beamdb";
  private static final String CASSANDRA_TABLE = "scientists";
  private static final String CASSANDRA_THRIFT_PORT_PROPERTY="cassandra.input.thrift.port";
  private static final String CASSANDRA_THRIFT_ADDRESS_PROPERTY="cassandra.input.thrift.address";
  private static final String CASSANDRA_PARTITIONER_CLASS_PROPERTY="cassandra.input.partitioner.class";
  private static final String CASSANDRA_KEYSPACE_PROPERTY="cassandra.input.keyspace";
  private static final String CASSANDRA_COLUMNFAMILY_PROPERTY="cassandra.input.columnfamily";
  private static final String CASSANDRA_PARTITIONER_CLASS_VALUE="Murmur3Partitioner";
>>>>>>> Changes as per Dipti's code review, revisit code
  private static HIFTestOptions options;

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(HIFTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HIFTestOptions.class);
<<<<<<< HEAD
    LOGGER.info("Pipeline created successfully with the options.");
<<<<<<< HEAD
    conf = getConfiguration(options);
  }

<<<<<<< HEAD
  /**
   * This test reads data from the Cassandra instance and verifies if data is read
   * successfully.
   * @throws Exception
   */
  @Test
  public void testHIFReadForCassandra() throws Exception {
    Pipeline pipeline = TestPipeline.create(options);
    SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
      private static final long serialVersionUID = 1L;
=======
	/**
	 * This test reads data from the Cassandra instance and verifies if data is
	 * read successfully.
	 *
	 * @throws Exception
	 */
	@Test
	public void testHIFReadForCassandra() {
		Pipeline pipeline = TestPipeline.create(options);
		SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
			private static final long serialVersionUID = 1L;
>>>>>>> Removed throws exception from Cassandra IT

      @Override
      public String apply(Row input) {
        return input.getString("y_id");
      }
    };
    PCollection<KV<Long, String>> cassandraData =
        pipeline.apply(HadoopInputFormatIO.<Long, String>read().withConfiguration(conf)
            .withValueTranslation(myValueTranslate));
    PAssert.thatSingleton(cassandraData.apply("Count", Count.<KV<Long, String>>globally()))
        .isEqualTo(1000L);
    pipeline.run();
  }

<<<<<<< HEAD
  /**
   * This test reads data from the Cassandra instance based on query and verifies if data is read successfully.
   * @throws Exception
   */
  @Test
  public void testHIFReadForCassandraQuery() throws Exception {
    Pipeline pipeline = TestPipeline.create(options);
    conf.set(
        "cassandra.input.cql",
        "select * from ycsb.usertable where token(y_id) > ? and token(y_id) <= ? and y_id='user3117720508089767496' allow filtering");
    SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
      private static final long serialVersionUID = 1L;
=======
	/**
	 * This test reads data from the Cassandra instance based on query and
	 * verifies if data is read successfully.
	 *
	 * @throws Exception
	 */
	@Test
	public void testHIFReadForCassandraQuery() {
		Pipeline pipeline = TestPipeline.create(options);
		conf.set(
				"cassandra.input.cql",
				"select * from ycsb.usertable where token(y_id) > ? and token(y_id) <= ? and y_id='user3117720508089767496' allow filtering");
		SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
			private static final long serialVersionUID = 1L;
>>>>>>> Removed throws exception from Cassandra IT

      @Override
      public String apply(Row input) {
        return input.getString("y_id");
      }
    };
    PCollection<KV<Long, String>> cassandraData =
        pipeline.apply(HadoopInputFormatIO.<Long, String>read().withConfiguration(conf)
            .withValueTranslation(myValueTranslate));
    PAssert.thatSingleton(cassandraData.apply("Count", Count.<KV<Long, String>>globally()))
        .isEqualTo(1L);

    pipeline.run();
  }

<<<<<<< HEAD
=======
=======
>>>>>>> Review comments followed in Cassandra IT
  }

  /**
   * This test reads data from the Cassandra instance and verifies if data is read successfully.
   */
  @Test
  public void testHIFReadForCassandra() {
    Pipeline pipeline = TestPipeline.create(options);
    Configuration conf = getConfiguration(options);
    SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
      @Override
      public String apply(Row input) {
        return input.getString("scientist");
      }
    };
    PCollection<KV<Long, String>> cassandraData = pipeline
                    .apply(HadoopInputFormatIO.<Long, String>read().withConfiguration(conf)
                        .withValueTranslation(myValueTranslate));
    PAssert.thatSingleton(cassandraData.apply("Count", Count.<KV<Long, String>>globally()))
        .isEqualTo(10L);

    List<KV<Long, String>> expectedResults =
        Arrays.asList(KV.of(1L, "Faraday"), KV.of(2L, "Newton"), KV.of(3L, "Galilei"),
            KV.of(4L, "Maxwell"), KV.of(5L, "Pasteur"), KV.of(6L, "Copernicus"),
            KV.of(7L, "Curie"), KV.of(8L, "Bohr"), KV.of(9L, "Darwin"), KV.of(10L, "Einstein"));
    PAssert.that(cassandraData).containsInAnyOrder(expectedResults);
    pipeline.run().waitUntilFinish();
  }

  /**
   * This test reads data from the Cassandra instance based on query and verifies if data is read
   * successfully.
   */
  @Test
  public void testHIFReadForCassandraQuery() {
    Pipeline pipeline = TestPipeline.create(options);
    Configuration conf = getConfiguration(options);
    conf.set("cassandra.input.cql", "select * from " + CASSANDRA_KEYSPACE + "." + CASSANDRA_TABLE
        + " where token(id) > ? and token(id) <= ? and scientist='Einstein' allow filtering");
    SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
      @Override
      public String apply(Row input) {
        return input.getString("id");
      }
    };
    PCollection<KV<Long, String>> cassandraData = pipeline
                    .apply(HadoopInputFormatIO.<Long, String>read()
                        .withConfiguration(conf)
                        .withValueTranslation(myValueTranslate));
    PAssert.thatSingleton(cassandraData.apply("Count", Count.<KV<Long, String>>globally()))
        .isEqualTo(1L);

    pipeline.run().waitUntilFinish();
  }

  /**
   * Returns configuration of CqlInutFormat. Mandatory parameters required apart from inputformat
   * class name, key class, value class are thrift port, thrift address, partitioner class, keyspace
   * and columnfamily name.
   */
>>>>>>> Elastic, Cassandra embedded code and ITs
  public static Configuration getConfiguration(HIFTestOptions options) {
    Configuration conf = new Configuration();
<<<<<<< HEAD
    conf.set("cassandra.input.thrift.port", options.getServerPort().toString());
    conf.set("cassandra.input.thrift.address", options.getServerIp());
    conf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");
    conf.set("cassandra.input.keyspace", CASSANDRA_KEYSPACE);
    conf.set("cassandra.input.columnfamily", CASSANDRA_TABLE);
<<<<<<< HEAD
<<<<<<< HEAD
    conf.setClass("mapreduce.job.inputformat.class",
        org.apache.cassandra.hadoop.cql3.CqlInputFormat.class, InputFormat.class);
    conf.setClass("key.class", java.lang.Long.class, Object.class);
    conf.setClass("value.class", com.datastax.driver.core.Row.class, Object.class);
    return conf;
  }
=======
	/**
	 * Returns configuration of CqlInutFormat. Mandatory parameters required
	 * apart from inputformat class name, key class, value class are thrift
	 * port, thrift address, partitioner class, keyspace and columnfamily name
	 *
	 */
	public static Configuration getConfiguration(HIFTestOptions options) {
		Configuration conf = new Configuration();
		conf.set("cassandra.input.thrift.port",
				String.format("%d", options.getServerPort()));
		conf.set("cassandra.input.thrift.address", options.getServerIp());
		conf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");
		conf.set("cassandra.input.keyspace", CASSANDRA_KEYSPACE);
		conf.set("cassandra.input.columnfamily", CASSANDRA_TABLE);
		conf.setClass(HadoopInputFormatIOContants.INPUTFORMAT_CLASSNAME,
				org.apache.cassandra.hadoop.cql3.CqlInputFormat.class,
				InputFormat.class);
		conf.setClass(HadoopInputFormatIOContants.KEY_CLASS,
				java.lang.Long.class, Object.class);
		conf.setClass(HadoopInputFormatIOContants.VALUE_CLASS,
				com.datastax.driver.core.Row.class, Object.class);
		return conf;
	}
>>>>>>> Removed throws exception from Cassandra IT
=======
    conf.setClass(HadoopInputFormatIOContants.INPUTFORMAT_CLASSNAME,
=======
=======
    conf.set(CASSANDRA_THRIFT_PORT_PROPERTY, options.getServerPort().toString());
    conf.set(CASSANDRA_THRIFT_ADDRESS_PROPERTY, options.getServerIp());
    conf.set(CASSANDRA_PARTITIONER_CLASS_PROPERTY, CASSANDRA_PARTITIONER_CLASS_VALUE);
    conf.set(CASSANDRA_KEYSPACE_PROPERTY, CASSANDRA_KEYSPACE);
    conf.set(CASSANDRA_COLUMNFAMILY_PROPERTY, CASSANDRA_TABLE);
>>>>>>> Changes as per Dipti's code review, revisit code
    conf.setClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME,
>>>>>>> Changes for spaces, Constants file name and comments as per Stephens code review comments
        org.apache.cassandra.hadoop.cql3.CqlInputFormat.class, InputFormat.class);
    conf.setClass(HadoopInputFormatIOConstants.KEY_CLASS, java.lang.Long.class, Object.class);
    conf.setClass(HadoopInputFormatIOConstants.VALUE_CLASS, com.datastax.driver.core.Row.class,
        Object.class);
    return conf;
  }
>>>>>>> Elastic, Cassandra embedded code and ITs
}
