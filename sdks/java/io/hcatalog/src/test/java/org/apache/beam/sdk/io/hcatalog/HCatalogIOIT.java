package org.apache.beam.sdk.io.hcatalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.hadoop.hive.conf.HiveConf;
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
    private static HiveConf hiveConf;

    @Rule
    public TestPipeline pipelineWrite = TestPipeline.create();
    @Rule
    public TestPipeline pipelineRead = TestPipeline.create();

    @BeforeClass
    public static void setup() {
        configProperties.put("hive.metastore.uris", "thrift://namenode:9083");
        configProperties.put("dfs.datanode.use.datanode.hostname", "true");
        configProperties.put("dfs.client.use.datanode.hostname", "true");
    }

    @Test
    public void testWriteThenRead() {

        runWrite();
        runRead();
    }

    private void runRead() {
        pipelineRead
                .apply(HCatalogIO.read()
                        .withConfigProperties(configProperties)
                        .withDatabase("default") //optional, assumes default if none specified
                        .withTable("mytable"));

        pipelineRead.run();
    }

    private void runWrite() {
        pipelineWrite
                .apply(Create.of(generateHCatRecords(1)))
                .apply(
                        HCatalogIO.write()
                                .withConfigProperties(configProperties)
                        .withTable("mytable")
                );

        pipelineWrite.run();
    }

    private List<HCatRecord> generateHCatRecords(int numRecords){
        List<HCatRecord> records = new ArrayList<HCatRecord>();
        for (int i = 0; i < numRecords; ++i) {

            DefaultHCatRecord record = new DefaultHCatRecord(1);

            records.add(record);
        }
        return records;
    }

}
