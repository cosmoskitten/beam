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
package org.apache.beam.runners.flink;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Charsets;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link FlinkJobServerDriver}. */
public class FlinkJobServerDriverTest {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobServerDriverTest.class);

  @Test
  public void testConfigurationDefaults() {
    FlinkJobServerDriver.FlinkServerConfiguration config =
        new FlinkJobServerDriver.FlinkServerConfiguration();
    assertThat(config.getHost(), is("localhost"));
    assertThat(config.getPort(), is(8099));
    assertThat(config.getArtifactPort(), is(8098));
    assertThat(config.getFlinkMasterUrl(), is("[auto]"));
    assertThat(config.getSdkWorkerParallelism(), is(1L));
    assertThat(config.isCleanArtifactsPerJob(), is(false));
    assertThat(config.isArtifactServiceDisabled(), is(false));
    FlinkJobServerDriver flinkJobServerDriver = FlinkJobServerDriver.fromConfig(config);
    assertThat(flinkJobServerDriver, is(not(nullValue())));
  }

  @Test
  public void testConfigurationFromArgs() {
    FlinkJobServerDriver driver =
        FlinkJobServerDriver.fromParams(
            new String[] {
              "--job-host=test",
              "--job-port",
              "42",
              "--artifact-port",
              "43",
              "--flink-master-url=jobmanager",
              "--sdk-worker-parallelism=4",
              "--clean-artifacts-per-job",
            });
    FlinkJobServerDriver.FlinkServerConfiguration config =
        (FlinkJobServerDriver.FlinkServerConfiguration) driver.configuration;
    assertThat(config.getHost(), is("test"));
    assertThat(config.getPort(), is(42));
    assertThat(config.getArtifactPort(), is(43));
    assertThat(config.getFlinkMasterUrl(), is("jobmanager"));
    assertThat(config.getSdkWorkerParallelism(), is(4L));
    assertThat(config.isCleanArtifactsPerJob(), is(true));
  }

  @Test
  public void testConfigurationFromConfig() {
    FlinkJobServerDriver.FlinkServerConfiguration config =
        new FlinkJobServerDriver.FlinkServerConfiguration();
    FlinkJobServerDriver driver = FlinkJobServerDriver.fromConfig(config);
    assertThat(driver.configuration, is(config));
  }

  private void testJobServerDriver(
      String[] inputParams, String[] expectedOutputs, String[] forbiddenOutputs) throws Exception {
    FlinkJobServerDriver driver = null;
    Thread driverThread = null;
    final PrintStream oldOut = System.err;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream newOut = new PrintStream(baos);
    try {
      System.setErr(newOut);
      driver = FlinkJobServerDriver.fromParams(inputParams);
      driverThread = new Thread(driver);
      driverThread.start();
      // wait for all expected outputs to appear
      while (true) {
        newOut.flush();
        String output = baos.toString(Charsets.UTF_8.name());
        for (String forbiddenOutput : forbiddenOutputs) {
          assertThat(output.contains(forbiddenOutput), is(false));
        }
        boolean success = true;
        for (String expectedOutput : expectedOutputs) {
          if (!output.contains(expectedOutput)) {
            success = false;
            break;
          }
        }
        if (success) {
          break;
        }
        Thread.sleep(100);
      }
      assertThat(driverThread.isAlive(), is(true));
    } catch (Throwable t) {
      // restore to print exception
      System.setErr(oldOut);
      throw t;
    } finally {
      System.setErr(oldOut);
      if (driver != null) {
        driver.stop();
      }
      if (driverThread != null) {
        driverThread.interrupt();
        driverThread.join();
      }
    }
  }

  @Test(timeout = 30_000)
  public void testJobServerDriver_withArtifactService() throws Exception {
    String[] inputParams = new String[] {"--job-port=0", "--artifact-port=0"};
    String[] expectedOutputs =
        new String[] {
          "JobService started on localhost:", "ArtifactStagingService started on localhost:"
        };
    testJobServerDriver(inputParams, expectedOutputs, new String[]{});
  }

  @Test(timeout = 30_000)
  public void testJobServerDriver_withoutArtifactService() throws Exception {
    String[] inputParams = new String[] {"--job-port=0", "--artifact-service-disabled"};
    String[] expectedOutputs = new String[] {"JobService started on localhost:"};
    String[] forbiddenOutputs = new String[] {"ArtifactStagingService started on localhost:"};
    testJobServerDriver(inputParams, expectedOutputs, forbiddenOutputs);
  }
}
