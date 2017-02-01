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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD:sdks/java/io/hadoop-input-format/src/test/java/org/apache/beam/sdk/io/hadoop/inputformat/unit/tests/HIFIOWithElasticTest.java
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
=======
>>>>>>> Modifications according to code review comments.:sdks/java/io/hadoop-input-format/src/test/java/org/apache/beam/sdk/io/hadoop/inputformat/HIFIOWithElasticTest.java
=======
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIOContants;
>>>>>>> Added tempfolder rule in elastic test, assert changes in cassandra IT, dependency version changes in pom.xml
=======
>>>>>>> Added tests with scientist data
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Tests to validate HadoopInputFormatIO for embedded Elasticsearch instance.
 *
 */
@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.JVM)
public class HIFIOWithElasticTest implements Serializable {

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> Embdded elastic test with value checks and query formatted
  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(HIFIOWithElasticTest.class);
  private static final String ELASTIC_IN_MEM_HOSTNAME = "127.0.0.1";
  private static final String ELASTIC_IN_MEM_PORT = "9200";
  private static final String ELASTIC_INTERNAL_VERSION = "5.x";
  private static final String TRUE = "true";
  private static final String ELASTIC_INDEX_NAME = "beamdb";
  private static final String ELASTIC_TYPE_NAME = "scientists";
  private static final String ELASTIC_RESOURCE = "/" + ELASTIC_INDEX_NAME + "/" + ELASTIC_TYPE_NAME;
<<<<<<< HEAD

  @BeforeClass
  public static void startServer()
      throws NodeValidationException, InterruptedException, IOException {
    ElasticEmbeddedServer.startElasticEmbeddedServer();
  }

  /**
   * Test to read data from embedded Elastic instance and verify whether data is read
   * successfully.
   */
  @Test
  public void testHifIOWithElastic() {
    TestPipeline p = TestPipeline.create();
    Configuration conf = getConfiguration();
    PCollection<KV<Text, MapWritable>> esData =
        p.apply(HadoopInputFormatIO.<Text, MapWritable>read().withConfiguration(conf));
    PCollection<Long> count = esData.apply(Count.<KV<Text, MapWritable>>globally());
    PAssert.thatSingleton(count).isEqualTo((long) 10);

    p.run().waitUntilFinish();
  }

  /**
   * Test to read data from embedded Elastic instance based on query and verify whether data is
   * read successfully.
   */
  @Test
  public void testHifIOWithElasticQuery() {
    TestPipeline p = TestPipeline.create();
    Configuration conf = getConfiguration();
    String query =
        "{\n" + "  \"query\": {\n"
              + "  \"match\" : {\n"
              + "    \"empid\" : {\n"
              + "      \"query\" : \"xyz22602\",\n"
              + "      \"type\" : \"boolean\"\n"
              + "    }\n"
              + "  }\n"
              + "  }\n"
              + "}";
    conf.set(ConfigurationOptions.ES_QUERY, query);
    PCollection<KV<Text, MapWritable>> esData =
        p.apply(HadoopInputFormatIO.<Text, MapWritable>read().withConfiguration(conf));
    PCollection<Long> count = esData.apply(Count.<KV<Text, MapWritable>>globally());
    PAssert.thatSingleton(count).isEqualTo((long) 1);

    p.run().waitUntilFinish();
  }

  public static Map<String, Object> populateElasticData(String empid, String name, Date joiningDate,
      String[] skills, String designation) {
    Map<String, Object> data = new HashMap<String, Object>();
    data.put("empid", empid);
    data.put("name", name);
    data.put("joiningDate", joiningDate);
    data.put("skills", skills);
    data.put("designation", designation);
    return data;
  }

  @AfterClass
  public static void shutdownServer() throws IOException {
    ElasticEmbeddedServer.shutdown();
  }

  /**
   * Class for in memory elastic server
   *
   */
  static class ElasticEmbeddedServer implements Serializable {

    private static final long serialVersionUID = 1L;
    private static Node node;
    private static final String DEFAULT_PATH = "target/ESData";

    public static void startElasticEmbeddedServer()
        throws UnknownHostException, NodeValidationException {

      Settings settings =
          Settings.builder().put("node.data", TRUE).put("network.host", ELASTIC_IN_MEM_HOSTNAME)
              .put("http.port", ELASTIC_IN_MEM_PORT).put("path.data", DEFAULT_PATH)
              .put("path.home", DEFAULT_PATH).put("transport.type", "local")
              .put("http.enabled", TRUE).put("node.ingest", TRUE).build();
      node = new PluginNode(settings);
      node.start();
      LOGGER.info("Elastic im memory server started..");
      prepareElasticIndex();
      LOGGER.info("Prepared index " + ELASTIC_INDEX_NAME
          + "and populated data on elastic in memory server..");
    }

    private static void prepareElasticIndex() {
      CreateIndexRequest indexRequest = new CreateIndexRequest(ELASTIC_INDEX_NAME);
      node.client().admin().indices().create(indexRequest).actionGet();
      for (int i = 0; i < 10; i++) {
        node.client().prepareIndex(ELASTIC_INDEX_NAME, ELASTIC_TYPE_NAME, String.valueOf(i))
            .setSource(populateElasticData("xyz2260" + i, "John Foo", new Date(),
                new String[] {"java"}, "Software engineer"))
            .execute();
      }
      GetResponse response = node.client().prepareGet(ELASTIC_INDEX_NAME, ELASTIC_TYPE_NAME, "1")
          .execute().actionGet();

    }

    public Client getClient() throws UnknownHostException {
      return node.client();
    }

    public static void shutdown() throws IOException {
      DeleteIndexRequest indexRequest = new DeleteIndexRequest(ELASTIC_INDEX_NAME);
      node.client().admin().indices().delete(indexRequest).actionGet();
      LOGGER.info("Deleted index " + ELASTIC_INDEX_NAME + " from elastic in memory server");
      node.close();
      LOGGER.info("Closed elastic in memory server node.");
      deleteElasticDataDirectory();
    }

    private static void deleteElasticDataDirectory() {
      try {
        FileUtils.deleteDirectory(new File(DEFAULT_PATH));
      } catch (IOException e) {
        throw new RuntimeException("Exception: Could not delete elastic data directory", e);
      }
    }

  }

  /**
   *
   * Class created for handling "http.enabled" property as "true" for elastic search node
   *
   */
  static class PluginNode extends Node implements Serializable {

    private static final long serialVersionUID = 1L;
    static Collection<Class<? extends Plugin>> list = new ArrayList<Class<? extends Plugin>>();
    static {
      list.add(Netty4Plugin.class);
    }

    public PluginNode(final Settings settings) {
      super(InternalSettingsPreparer.prepareEnvironment(settings, null), list);

    }
  }

  public Configuration getConfiguration() {
    Configuration conf = new Configuration();

    conf.set(ConfigurationOptions.ES_NODES, ELASTIC_IN_MEM_HOSTNAME);
    conf.set(ConfigurationOptions.ES_PORT, String.format("%s", ELASTIC_IN_MEM_PORT));
    conf.set(ConfigurationOptions.ES_RESOURCE, ELASTIC_RESOURCE);
    conf.set("es.internal.es.version", ELASTIC_INTERNAL_VERSION);
    conf.set(ConfigurationOptions.ES_NODES_DISCOVERY, TRUE);
    conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, TRUE);
    conf.setClass("mapreduce.job.inputformat.class",
        org.elasticsearch.hadoop.mr.EsInputFormat.class, InputFormat.class);
    conf.setClass("key.class", Text.class, Object.class);
    conf.setClass("value.class", MapWritable.class, Object.class);
    conf.setClass("mapred.mapoutput.value.class", MapWritable.class, Object.class);
    return conf;
  }
=======
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(HIFIOWithElasticTest.class);
	private static final String ELASTIC_IN_MEM_HOSTNAME = "127.0.0.1";
	private static final String ELASTIC_IN_MEM_PORT = "9200";
	private static final String ELASTIC_INTERNAL_VERSION = "5.x";
	private static final String TRUE = "true";
	private static final String ELASTIC_INDEX_NAME = "xyz";
	private static final String ELASTIC_TYPE_NAME = "employee";
	private static final String ELASTIC_RESOURCE = "/" + ELASTIC_INDEX_NAME
			+ "/" + ELASTIC_TYPE_NAME;
    private static final int SIZE = 1000;
=======
  private static final int SIZE = 10;
<<<<<<< HEAD
  private static final String ELASTIC_TYPE_ID_PREFIX = "xyz22600";
>>>>>>> Embdded elastic test with value checks and query formatted
=======
  private static final String ELASTIC_TYPE_ID_PREFIX = "s";
>>>>>>> Added tests with scientist data

  @ClassRule
  public static TemporaryFolder elasticTempFolder = new TemporaryFolder();

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void startServer() throws NodeValidationException, InterruptedException,
      IOException {
    ElasticEmbeddedServer.startElasticEmbeddedServer();
  }

  /**
   * Test to read data from embedded Elasticsearch instance and verify whether data is read
   * successfully.
   */
  @Test
  public void testHifIOWithElastic() {
    Configuration conf = getConfiguration();

    PCollection<KV<Text, LinkedMapWritable>> esData =
        pipeline.apply(HadoopInputFormatIO.<Text, LinkedMapWritable>read().withConfiguration(conf));
    PCollection<Long> count = esData.apply(Count.<KV<Text, LinkedMapWritable>>globally());
    PAssert.thatSingleton(count).isEqualTo((long) SIZE);
    PCollection<LinkedMapWritable> values = esData.apply(Values.<LinkedMapWritable>create());

    MapElements<LinkedMapWritable, String> transformFunc =
        MapElements.<LinkedMapWritable, String>via(new SimpleFunction<LinkedMapWritable, String>() {
          @Override
          public String apply(LinkedMapWritable mapw) {
            Text text = (Text) mapw.get(new Text("id"));
            return text != null ? text.toString() : "";
          }
        });

    PCollection<String> textValues = values.apply(transformFunc);
    List<String> expectedResults = new ArrayList<>();
    for (int cnt = 0; cnt < SIZE; cnt++) {
      expectedResults.add(ELASTIC_TYPE_ID_PREFIX + cnt);
    }
    PAssert.that(textValues).containsInAnyOrder(expectedResults);
    pipeline.run().waitUntilFinish();
  }

  /**
   * Test to read data from embedded Elasticsearch instance based on query and verify whether data
   * is read successfully.
   */
  @Test
  public void testHifIOWithElasticQuery() {
    Configuration conf = getConfiguration();
    String fieldValue = ELASTIC_TYPE_ID_PREFIX + "2";
    String query =
        "{\n"
            + "  \"query\": {\n"
            + "  \"match\" : {\n"
            + "    \"id\" : {\n"
            + "      \"query\" : \"" + fieldValue + "" + "\",\n"
            + "      \"type\" : \"boolean\"\n"
            + "    }\n"
            + "  }\n"
            + "  }\n"
            + "}";
    conf.set(ConfigurationOptions.ES_QUERY, query);
    PCollection<KV<Text, LinkedMapWritable>> esData =
        pipeline.apply(HadoopInputFormatIO.<Text, LinkedMapWritable>read().withConfiguration(conf));
    PCollection<Long> count = esData.apply(Count.<KV<Text, LinkedMapWritable>>globally());
    PAssert.thatSingleton(count).isEqualTo((long) 1);

    pipeline.run().waitUntilFinish();
  }

  public static Map<String, String> populateElasticData(String id, String name) {
    Map<String, String> data = new HashMap<String, String>();
    data.put("id", id);
    data.put("scientist", name);
    return data;
  }

  @AfterClass
  public static void shutdownServer() throws IOException {
    ElasticEmbeddedServer.shutdown();
  }

  /**
   * Class for in memory Elasticsearch server
   */
  static class ElasticEmbeddedServer implements Serializable {

    private static final long serialVersionUID = 1L;
    private static Node node;

    public static void startElasticEmbeddedServer() throws UnknownHostException,
        NodeValidationException, InterruptedException {

      Settings settings =
          Settings.builder().put("node.data", TRUE).put("network.host", ELASTIC_IN_MEM_HOSTNAME)
              .put("http.port", ELASTIC_IN_MEM_PORT).put("path.data", elasticTempFolder.getRoot().getPath())
              .put("path.home", elasticTempFolder.getRoot().getPath()).put("transport.type", "local")
              .put("http.enabled", TRUE).put("node.ingest", TRUE).build();
      node = new PluginNode(settings);
      node.start();
      LOGGER.info("Elastic im memory server started..");
      prepareElasticIndex();
      LOGGER.info("Prepared index " + ELASTIC_INDEX_NAME
          + "and populated data on elastic in memory server..");
    }

    private static void prepareElasticIndex() throws InterruptedException {
      CreateIndexRequest indexRequest = new CreateIndexRequest(ELASTIC_INDEX_NAME);
      node.client().admin().indices().create(indexRequest).actionGet();
      for (int i = 0; i < SIZE; i++) {
        node.client()
            .prepareIndex(ELASTIC_INDEX_NAME, ELASTIC_TYPE_NAME, String.valueOf(i))
            .setSource(
                populateElasticData(ELASTIC_TYPE_ID_PREFIX + i, "Faraday")).execute();
        Thread.sleep(100);
      }
      GetResponse response =
          node.client().prepareGet(ELASTIC_INDEX_NAME, ELASTIC_TYPE_NAME, "1").execute()
              .actionGet();

    }

    public Client getClient() throws UnknownHostException {
      return node.client();
    }

    public static void shutdown() throws IOException {
      DeleteIndexRequest indexRequest = new DeleteIndexRequest(ELASTIC_INDEX_NAME);
      node.client().admin().indices().delete(indexRequest).actionGet();
      LOGGER.info("Deleted index " + ELASTIC_INDEX_NAME + " from elastic in memory server");
      node.close();
      LOGGER.info("Closed elastic in memory server node.");
      deleteElasticDataDirectory();
    }

    private static void deleteElasticDataDirectory() {
      try {
        FileUtils.deleteDirectory(new File(elasticTempFolder.getRoot().getPath()));
      } catch (IOException e) {
        throw new RuntimeException("Exception: Could not delete elastic data directory", e);
      }
    }

  }

  /**
   *
   * Class created for handling "http.enabled" property as "true" for Elasticsearch node.
   *
   */
  static class PluginNode extends Node implements Serializable {

    private static final long serialVersionUID = 1L;
    static Collection<Class<? extends Plugin>> list = new ArrayList<Class<? extends Plugin>>();
    static {
      list.add(Netty4Plugin.class);
    }

<<<<<<< HEAD
		conf.set(ConfigurationOptions.ES_NODES, ELASTIC_IN_MEM_HOSTNAME);
		conf.set(ConfigurationOptions.ES_PORT,
				String.format("%s", ELASTIC_IN_MEM_PORT));
		conf.set(ConfigurationOptions.ES_RESOURCE, ELASTIC_RESOURCE);
		conf.set("es.internal.es.version", ELASTIC_INTERNAL_VERSION);
		conf.set(ConfigurationOptions.ES_NODES_DISCOVERY, TRUE);
		conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, TRUE);
		conf.setClass(HadoopInputFormatIOContants.INPUTFORMAT_CLASSNAME,
				org.elasticsearch.hadoop.mr.EsInputFormat.class,
				InputFormat.class);
		conf.setClass(HadoopInputFormatIOContants.KEY_CLASS, Text.class,
				Object.class);
		conf.setClass(HadoopInputFormatIOContants.VALUE_CLASS,
				MapWritable.class, Object.class);
		conf.setClass("mapred.mapoutput.value.class", MapWritable.class,
				Object.class);
		return conf;
	}
>>>>>>> Tests modifications.
=======
    public PluginNode(final Settings settings) {
      super(InternalSettingsPreparer.prepareEnvironment(settings, null), list);

    }
  }

  /**
   * Set the Elasticsearch configuration parameters in the Hadoop configuration object.
   * Configuration object should have InputFormat class, key class and value class to be set.
   * Mandatory fields for ESInputFormat to be set are es.resource, es.nodes, es.port,
   * es.internal.es.version. Please refer <a
   * href="https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html"
   * >Elasticsearch Configuration</a> for more details.
   */
  public Configuration getConfiguration() {
    Configuration conf = new Configuration();

    conf.set(ConfigurationOptions.ES_NODES, ELASTIC_IN_MEM_HOSTNAME);
    conf.set(ConfigurationOptions.ES_PORT, String.format("%s", ELASTIC_IN_MEM_PORT));
    conf.set(ConfigurationOptions.ES_RESOURCE, ELASTIC_RESOURCE);
    conf.set("es.internal.es.version", ELASTIC_INTERNAL_VERSION);
    conf.set(ConfigurationOptions.ES_NODES_DISCOVERY, TRUE);
    conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, TRUE);
    conf.setClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME,
        org.elasticsearch.hadoop.mr.EsInputFormat.class, InputFormat.class);
    conf.setClass(HadoopInputFormatIOConstants.KEY_CLASS, Text.class, Object.class);
    conf.setClass(HadoopInputFormatIOConstants.VALUE_CLASS, LinkedMapWritable.class, Object.class);
    return conf;
  }
>>>>>>> Embdded elastic test with value checks and query formatted
}
