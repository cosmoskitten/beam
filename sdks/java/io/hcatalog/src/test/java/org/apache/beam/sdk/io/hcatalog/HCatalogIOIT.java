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
package org.apache.beam.sdk.io.hcatalog;

import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.getExpectedRecords;
import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.getHCatRecords;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/**
 * IOIT test to run HCatalog.
 * <p>You can run tests on prepared hCatalog infrastructure.</p>
 * <p>To run test specify number of records (numberOfRecords) to write to HCatalog,
 * metastore url (HCatalogMetastoreUri) and metastore port (HCatalogMetastorePort).</p>
 * <pre>{@code mvn clean verify -Pio-it -pl sdks/java/io/hcatalog/
 * -DintegrationTestPipelineOptions=
 * '[
 * "--tempRoot=gs://url",
 * "--runner=TestDataflowRunner",
 * "--numberOfRecords=1024",
 * "--HCatalogMetastoreUri=hcatalog-metastore",
 * "--HCatalogMetastorePort=9083"
 * ]'
 * }</pre>
 */

@RunWith(JUnit4.class)
public class HCatalogIOIT {

    private static Map<String, String> configProperties = new HashMap<String, String>();
    private static Integer numberOfRecords;
    private static String metastoreUri;
    private static Integer port;
    private static String host;

    @Rule
    public TestPipeline pipelineWrite = TestPipeline.create();
    @Rule
    public TestPipeline pipelineRead = TestPipeline.create();

    @BeforeClass
    public static void setup() {
        PipelineOptionsFactory.register(IOTestPipelineOptions.class);
        IOTestPipelineOptions options = TestPipeline.testingPipelineOptions()
                .as(IOTestPipelineOptions.class);

        numberOfRecords = options.getNumberOfRecords();
        host = options.getHCatalogMetastoreUri();
        port = options.getHCatalogMetastorePort();
        metastoreUri = String.format("thrift://%s:%s", host, port);
        configProperties.put("hive.metastore.uris", metastoreUri);

    }

    @Test
    public void writeAndReadAll() throws Exception {
       pipelineWrite
                .apply("Generate sequence", Create.of(getHCatRecords(numberOfRecords)))
                .apply(
                HCatalogIO.write()
                        .withConfigProperties(configProperties)
                        .withTable("mytable")
        );
       pipelineWrite.run();


        PCollection<String> testRecords = pipelineRead
                .apply(HCatalogIO.read()
                    .withConfigProperties(configProperties)
                    .withDatabase("default")
                    .withTable("mytable")
                )
                .apply(ParDo.of(new CreateHCatFn()));
        PAssert.that(testRecords).containsInAnyOrder(getExpectedRecords(numberOfRecords));
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

