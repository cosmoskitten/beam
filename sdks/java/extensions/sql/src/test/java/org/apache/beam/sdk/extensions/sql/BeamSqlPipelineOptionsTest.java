package org.apache.beam.sdk.extensions.sql;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamSqlPipelineOptionsTest}. */
@RunWith(JUnit4.class)
public class BeamSqlPipelineOptionsTest {

  @Test
  public void testPlannerImplClassName() {
    BeamSqlPipelineOptions options =
        PipelineOptionsFactory.create().as(BeamSqlPipelineOptions.class);
    assertEquals("org.apache.calcite.prepare.PlannerImpl", options.getPlannerImplClassName());

    options.setPlannerImplClassName("org.apache.test");
    assertEquals("org.apache.test", options.getPlannerImplClassName());
  }
}
