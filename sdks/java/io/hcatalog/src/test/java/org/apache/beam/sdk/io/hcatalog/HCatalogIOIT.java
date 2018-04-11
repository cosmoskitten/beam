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
package org.apache.beam.sdk.io.hcatalog;

import static org.apache.beam.sdk.io.common.IOITHelper.getHashForRecordCount;
import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.getHCatRecords;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/**
 * IOIT test to run HCatalog.
 * <p>You can run tests on prepared hCatalog infrastructure.</p>
 * <p>To run test specify number of records (numberOfRecords) to write to HCatalog,
 * metastore url (HCatalogMetastoreHostName),
 * metastore port (HCatalogMetastorePort),
 * hive port (HCatalogHivePort)
 * and hive database (HCatalogHiveDatabase)
 * to create a hive test table.</p>
 * <pre>{@code mvn clean verify -Pio-it -pl sdks/java/io/hcatalog/
 * -DintegrationTestPipelineOptions=
 * '[
 * "--tempRoot=gs://url",
 * "--runner=TestDataflowRunner",
 * "--numberOfRecords=100",
 * "--HCatalogMetastoreHostName=hcatalog-metastore",
 * "--HCatalogMetastorePort=9083"
 * "--HCatalogHivePort=10000"
 * "--HCatalogHiveDatabaseName=default"
 * ]'
 * }</pre>
 */

@RunWith(JUnit4.class)
public class HCatalogIOIT {

  private static final Map<Integer, String> EXPECTED_HASHES = ImmutableMap.of(
      100, "34c19971bd34cc1ed6218b84d0db3018",
      1000, "2db7f961724848ffcea299075c166ae8",
      10_000, "7885cdda3ed927e17f7db330adcbebcc"
  );

  private static String tableName;
  private static HiveDatabaseTestHelper helper;
  private static Map<String, String> configProperties;
  private static Integer numberOfRecords;
  private static String metastoreUri;
  private static Integer metastorePort;
  private static Integer hivePort;
  private static String hostName;
  private static String databaseName;

  @Rule
  public TestPipeline pipelineWrite = TestPipeline.create();
  @Rule
  public TestPipeline pipelineRead = TestPipeline.create();

  @BeforeClass
  public static void setup() throws Exception {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    IOTestPipelineOptions options = TestPipeline.testingPipelineOptions()
        .as(IOTestPipelineOptions.class);

    numberOfRecords = options.getNumberOfRecords();
    hostName = options.getHCatalogMetastoreHostName();
    metastorePort = options.getHCatalogMetastorePort();
    metastoreUri = String.format("thrift://%s:%s", hostName, metastorePort);

    configProperties = ImmutableMap.of("hive.metastore.uris", metastoreUri);

    hivePort = options.getHCatalogHivePort();
    databaseName = options.getHCatalogHiveDatabaseName();
    helper = new HiveDatabaseTestHelper(
        hostName,
        hivePort,
        databaseName);

    try {
      tableName = helper.createHiveTable("HCatalogIOIT");
    } catch (Exception e) {
      helper.closeConnection();
      throw new Exception("Problem with creating table. " + e);
    }
  }

  @After
  public void tearDown() throws Exception {
    try {
      helper.dropHiveTable(tableName);
    } catch (Exception e) {
      helper.closeConnection();
      throw new Exception("Problem with deleting database.");
    } finally {
      helper.closeConnection();
    }
  }

  @Test
  public void writeAndReadAll() throws Exception {
    pipelineWrite
        .apply("Generate sequence", Create.of(getHCatRecords(numberOfRecords)))
        .apply(
            HCatalogIO.write()
                .withConfigProperties(configProperties)
                .withDatabase(databaseName)
                .withTable(tableName)
        );
    pipelineWrite.run();


    PCollection<String> testRecords = pipelineRead
        .apply(HCatalogIO.read()
            .withConfigProperties(configProperties)
            .withDatabase(databaseName)
            .withTable(tableName)
        )
        .apply(ParDo.of(new CreateHCatFn()));

    PCollection<String> consolidatedHashcode = testRecords
        .apply("Calculate hashcode", Combine.globally(new HashingFn()));

    String expectedHash = getHashForRecordCount(numberOfRecords, EXPECTED_HASHES);
    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    pipelineRead.run();
  }


  /**
   * Outputs value stored in the HCatRecord.
   */
  public static class CreateHCatFn extends DoFn<HCatRecord, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().get(0).toString());
    }
  }


}

