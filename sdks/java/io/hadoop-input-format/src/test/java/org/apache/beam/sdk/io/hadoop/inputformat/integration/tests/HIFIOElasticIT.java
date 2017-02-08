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

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
<<<<<<< HEAD
=======
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIOConstants;
>>>>>>> Changes for spaces, Constants file name and comments as per Stephens code review comments
import org.apache.beam.sdk.io.hadoop.inputformat.custom.options.HIFTestOptions;
import org.apache.beam.sdk.io.hadoop.inputformat.testing.HashingFn;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
<<<<<<< HEAD
 * Runs integration test to validate HadoopInputFromatIO for an Elastic instance on GCP.
 *
 * You need to pass elastic server IP and port in beamTestPipelineOptions
=======
 * Runs integration test to validate HadoopInputFromatIO for an Elasticsearch instance.
 *
 * You need to pass Elasticsearch server IP and port in beamTestPipelineOptions.
>>>>>>> Implemented review comments for elastic IT
 *
 * <p>
 * You can run just this test by doing the following: mvn test-compile compile
 * failsafe:integration-test -D beamTestPipelineOptions='[ "--serverIp=1.2.3.4",
 * "--serverPort=<port>" ]' -Dit.test=HIFIOElasticIT -DskipITs=false
 */
@RunWith(JUnit4.class)
public class HIFIOElasticIT implements Serializable {

  static Logger logger = Logger.getLogger("HIFChecksumEvaluator");
  private static final String ELASTIC_INTERNAL_VERSION = "5.x";
  private static final String TRUE = "true";
  private static final String ELASTIC_INDEX_NAME = "test_data";
  private static final String ELASTIC_TYPE_NAME = "test_type";
  private static final String ELASTIC_RESOURCE = "/" + ELASTIC_INDEX_NAME + "/" + ELASTIC_TYPE_NAME;
  private static HIFTestOptions options;
  private static final long TEST_DATA_ROW_COUNT = 1000L;

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(HIFTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HIFTestOptions.class);
  }

  /**
<<<<<<< HEAD
   * This test reads data from the elastic instance and verifies whether data is read successfully.
=======
   * This test reads data from the Elasticsearch instance and verifies whether data is read
   * successfully.
<<<<<<< HEAD
>>>>>>> Elastic, Cassandra embedded code and ITs
=======
   * @throws IOException 
   * @throws SecurityException 
>>>>>>> Check in with changes for value checksum comparison
   */
  @Test
  public void testHifIOWithElastic() throws SecurityException, IOException {
    FileHandler fh = new FileHandler("ElasticIT.log");
    logger.addHandler(fh);
    // Expected hashcode is evaluated during insertion time one time and hardcoded here.
    String expectedHashCode = "7373697a12faa08be32104f67cf7ec2be2e20a1f";
    Pipeline pipeline = TestPipeline.create(options);
    Configuration conf = getConfiguration(options);
    PCollection<KV<Text, LinkedMapWritable>> esData =
        pipeline.apply(HadoopInputFormatIO.<Text, LinkedMapWritable>read().withConfiguration(conf));
    // Verify that the count of objects fetched using HIFInputFormat IO is correct.
    PCollection<Long> count = esData.apply(Count.<KV<Text, LinkedMapWritable>>globally());
    PAssert.thatSingleton(count).isEqualTo(TEST_DATA_ROW_COUNT);

    PCollection<LinkedMapWritable> values = esData.apply(Values.<LinkedMapWritable>create());
    MapElements<LinkedMapWritable, String> transformFunc =
        MapElements.<LinkedMapWritable, String>via(new SimpleFunction<LinkedMapWritable, String>() {
          @Override
          public String apply(LinkedMapWritable mapw) {
            String rowValue = "";
            rowValue = addFieldValuesToRow(rowValue, mapw, "User_Name");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Item_Code");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Txn_ID");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Item_ID");
            rowValue = addFieldValuesToRow(rowValue, mapw, "last_updated");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Price");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Title");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Description");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Age");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Item_Name");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Item_Price");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Availability");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Batch_Num");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Last_Ordered");
            rowValue = addFieldValuesToRow(rowValue, mapw, "City");
            logger.info("Row Value: " + rowValue);
            return rowValue;
          }
        });

    PCollection<String> textValues = values.apply(transformFunc);
    // Verify the output values using checksum comparison.
    PCollection<String> consolidatedHashcode =
        textValues.apply(Combine.globally(new HashingFn()).withoutDefaults());
    PAssert.that(consolidatedHashcode).containsInAnyOrder(expectedHashCode);
    pipeline.run().waitUntilFinish();

  }

  private String addFieldValuesToRow(String row, MapWritable mapw, String columnName) {
    Object valueObj = (Object) mapw.get(new Text(columnName));
    row += valueObj.toString() + "|";

    return row;
  }

  /**
<<<<<<< HEAD
   * This test reads data from the elastic instance based on a query and verifies if data is read
   * successfully.
=======
   * This test reads data from the Elasticsearch instance based on a query and verifies if data is
   * read successfully.
>>>>>>> Elastic, Cassandra embedded code and ITs
   */
  @Test
  public void testHifIOWithElasticQuery() {
    String expectedHashCode = "bbec8c2a39655de29b96d6069cef016db53d36a7";
    Long expectedRecords=1L;
    Pipeline pipeline = TestPipeline.create(options);
    Configuration conf = getConfiguration(options);
    String query =
        "{" + "  \"query\": {" + "  \"match\" : {" + "    \"Title\" : {"
            + "      \"query\" : \"M9u5xcAR\"," + "      \"type\" : \"boolean\"" + "    }" + "  }"
            + "  }" + "}";
    conf.set(ConfigurationOptions.ES_QUERY, query);
    PCollection<KV<Text, LinkedMapWritable>> esData =
        pipeline.apply(HadoopInputFormatIO.<Text, LinkedMapWritable>read().withConfiguration(conf));
    PCollection<Long> count = esData.apply(Count.<KV<Text, LinkedMapWritable>>globally());
    // Verify that the count of objects fetched using HIFInputFormat IO is correct.
    PAssert.thatSingleton(count).isEqualTo(expectedRecords);
    PCollection<LinkedMapWritable> values = esData.apply(Values.<LinkedMapWritable>create());
    MapElements<LinkedMapWritable, String> transformFunc =
        MapElements.<LinkedMapWritable, String>via(new SimpleFunction<LinkedMapWritable, String>() {
          @Override
          public String apply(LinkedMapWritable mapw) {
            String rowValue = "";
            rowValue = addFieldValuesToRow(rowValue, mapw, "User_Name");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Item_Code");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Txn_ID");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Item_ID");
            rowValue = addFieldValuesToRow(rowValue, mapw, "last_updated");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Price");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Title");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Description");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Age");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Item_Name");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Item_Price");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Availability");
            rowValue = addFieldValuesToRow(rowValue, mapw, "last_updated");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Last_Ordered");
            rowValue = addFieldValuesToRow(rowValue, mapw, "City");
            rowValue = addFieldValuesToRow(rowValue, mapw, "Country");
            return rowValue;
          }
        });

    PCollection<String> textValues = values.apply(transformFunc);
    // Verify the output values using checksum comparison.
    PCollection<String> consolidatedHashcode =
        textValues.apply(Combine.globally(new HashingFn()).withoutDefaults());
    PAssert.that(consolidatedHashcode).containsInAnyOrder(expectedHashCode);
    pipeline.run().waitUntilFinish();
  }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
 /**
	 * Set the Elasticsearch configuration parameters in the Hadoop
	 * configuration object. Configuration object should have InputFormat class,
	 * key class and value class to be set Mandatory fields for ESInputFormat to
	 * be set are es.resource, es.nodes, es.port, es.internal.es.version
	 */
>>>>>>> Modified elastic integration test.
=======
  /**
   * Set the Elasticsearch configuration parameters in the Hadoop configuration object.
   * Configuration object should have InputFormat class, key class and value class to be set.
   * Mandatory fields for ESInputFormat to be set are es.resource, es.nodes, es.port,
   * es.internal.es.version, es.nodes.wan.only. Please refer <a
   * href="https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html"
   * >Elasticsearch Configuration</a> for more details.
   */
>>>>>>> Elastic, Cassandra embedded code and ITs
  public static Configuration getConfiguration(HIFTestOptions options) {
=======
  /*
   * Returns Hadoop configuration for reading data from Elasticsearch. Configuration object should
   * have InputFormat class, key class and value class to be set. Mandatory fields for ESInputFormat
   * to be set are es.resource, es.nodes, es.port, es.internal.es.version, es.nodes.wan.only. Please
   * refer <a href="https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html"
   * >Elasticsearch Configuration</a> for more details.
   */
  private static Configuration getConfiguration(HIFTestOptions options) {
>>>>>>> Minor modifications.
    Configuration conf = new Configuration();

    conf.set(ConfigurationOptions.ES_NODES, options.getServerIp());
    conf.set(ConfigurationOptions.ES_PORT, options.getServerPort().toString());
    conf.set(ConfigurationOptions.ES_NODES_WAN_ONLY, TRUE);
    conf.set(ConfigurationOptions.ES_RESOURCE, ELASTIC_RESOURCE);
    conf.set("es.internal.es.version", ELASTIC_INTERNAL_VERSION);
    conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, TRUE);
<<<<<<< HEAD
    conf.setClass("mapreduce.job.inputformat.class",
        org.elasticsearch.hadoop.mr.EsInputFormat.class, InputFormat.class);
<<<<<<< HEAD
    conf.setClass("key.class", Text.class, Object.class);
    conf.setClass("value.class", MapWritable.class, Object.class);
    conf.setClass("mapred.mapoutput.value.class", MapWritable.class, Object.class);

=======
    conf.setClass(HadoopInputFormatIOContants.KEY_CLASS, Text.class, Object.class);
    conf.setClass(HadoopInputFormatIOContants.VALUE_CLASS, LinkedMapWritable.class, Object.class);
>>>>>>> Fix of issue extra bytes for LinkedMapWritable while deserializing
=======
    conf.setClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME,
        org.elasticsearch.hadoop.mr.EsInputFormat.class, InputFormat.class);
    conf.setClass(HadoopInputFormatIOConstants.KEY_CLASS, Text.class, Object.class);
    conf.setClass(HadoopInputFormatIOConstants.VALUE_CLASS, LinkedMapWritable.class, Object.class);
>>>>>>> Changes for spaces, Constants file name and comments as per Stephens code review comments
    return conf;
  }
}
