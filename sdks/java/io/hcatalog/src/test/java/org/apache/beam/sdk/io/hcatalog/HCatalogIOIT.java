package org.apache.beam.sdk.io.hcatalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * TODO.
 */

@RunWith(JUnit4.class)
public class HCatalogIOIT {

    private static Map<String, String> configProperties = new HashMap<String, String>();
    private static Integer numberOfRecords;

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

        configProperties.put("hive.metastore.uris", "thrift://localhost:9083");
    }

    @Test
    public void write() {
        runWrite();
    }

    @Test
    public void read() {
        runRead();
    }

    @Test
    public void writeAndReadAll(){
        PCollection<String> testRecords = pipelineWrite
                .apply("Generate sequence", GenerateSequence.from(0).to(numberOfRecords) )
                .apply("Generate hcat records", MapElements.via()))
    }

    private void runRead() {
        pipelineRead
                .apply(HCatalogIO.read()
                        .withConfigProperties(configProperties)
                        .withDatabase("default")
                        .withTable("mytable"));

        pipelineRead.run();
    }

    private void runWrite() {
        pipelineWrite
                .apply(Create.of(generateHCatRecords(numberOfRecords)))
                .apply(
                        HCatalogIO.write()
                                .withConfigProperties(configProperties)
                                .withTable("mytable")
                                .withPartition(new java.util.HashMap<>())
                                .withBatchSize(1024L)
                );

        pipelineWrite.run();
    }

    private List<HCatRecord> generateHCatRecords(int numRecords) {
        List<HCatRecord> records = new ArrayList<HCatRecord>();
        for (int i = 0; i < numRecords; ++i) {

            DefaultHCatRecord record = new DefaultHCatRecord(1);

            records.add(record);
        }
        return records;
    }

}
