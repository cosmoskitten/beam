package org.apache.beam.examples.complete;

import org.apache.beam.examples.complete.AutoComplete.Options;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AutocompleteIT {

  public static final String DEFAULT_INPUT = "gs://dataflow-samples/shakespeare/kinglear.txt";

  public static final Long DEFAULT_INPUT_CHECKSUM = 1;


  public interface AutocompleteITOptions extends TestPipelineOptions, Options {}

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }
a
  @Test
  public void testE2EAutoComplete() throws Exception {
    AutocompleteITOptions options = TestPipeline.testingPipelineOptions().as(AutocompleteITOptions.class);

    options.setInputFile(DEFAULT_INPUT);
    options.setOutputToBigQuery(false);
    options.setOutputToDatastore(false);
    options.setOutputToChecksum(true);
    options.setExpectedChecksum(DEFAULT_INPUT_CHECKSUM);


    AutoComplete.runAutocompletePipeline(options);
  }

}
