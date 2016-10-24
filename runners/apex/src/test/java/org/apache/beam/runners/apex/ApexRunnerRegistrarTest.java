package org.apache.beam.runners.apex;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

/**
 * Tests the proper registration of the Apex runner.
 */
public class ApexRunnerRegistrarTest {

    @Test
    public void testFullName() {
        String[] args =
                new String[] {String.format("--runner=%s", ApexRunner.class.getName())};
        PipelineOptions opts = PipelineOptionsFactory.fromArgs(args).create();
        assertEquals(opts.getRunner(), ApexRunner.class);
    }

    @Test
    public void testClassName() {
        String[] args =
                new String[] {String.format("--runner=%s", ApexRunner.class.getSimpleName())};
        PipelineOptions opts = PipelineOptionsFactory.fromArgs(args).create();
        assertEquals(opts.getRunner(), ApexRunner.class);
    }

}
