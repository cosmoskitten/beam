package org.apache.beam.sdk.options;

import org.apache.beam.sdk.annotations.Experimental;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link ExperimentalOptions}. */
@RunWith(JUnit4.class)
public class ExperimentalOptionsTest {
  @Test
  public void testExperimentIsSet() {
    ExperimentalOptions options =
        PipelineOptionsFactory.fromArgs("--experiments=experimentA,experimentB")
            .as(ExperimentalOptions.class);
    assertTrue(ExperimentalOptions.hasExperiment(options, "experimentA"));
    assertTrue(ExperimentalOptions.hasExperiment(options, "experimentB"));
    assertFalse(ExperimentalOptions.hasExperiment(options, "experimentC"));
  }
}
