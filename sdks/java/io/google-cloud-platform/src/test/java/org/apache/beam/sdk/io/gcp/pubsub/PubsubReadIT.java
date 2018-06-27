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
package org.apache.beam.sdk.io.gcp.pubsub;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for PubsubIO. */
@RunWith(JUnit4.class)
public class PubsubReadIT {

  @Rule public transient TestPubsubSignal signal = TestPubsubSignal.create();
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testReadPublicData() throws Exception {
    // The pipeline will never terminate on its own
    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);

    PCollection<PubsubMessage> messages =
        pipeline.apply(
            PubsubIO.readMessages()
                .fromTopic("projects/pubsub-public-data/topics/taxirides-realtime"));

    messages.apply(
        "waitForAnyMessage", signal.signalSuccessWhen(messages.getCoder(), anyMessage -> true));

    Duration timeout =
        pipeline.getOptions().getRunner().getSimpleName().endsWith("DataflowRunner")
            ? Duration.standardMinutes(5)
            : Duration.standardSeconds(30);
    pipeline.run();

    signal.waitForSuccess(timeout);
  }
}
