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
package org.apache.beam.examples.complete.game;

import static com.google.common.base.MoreObjects.firstNonNull;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.examples.complete.game.utils.WriteToBigQuery.FieldInfo;
import org.apache.beam.examples.complete.game.utils.WriteWindowedToBigQuery;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

/**
 * This class is part of a series of pipelines that tell a story in a 'gaming' domain. Concepts
 * include: stateful processing.
 *
 * <p>This pipeline processes an unbounded stream of 'game events'. It uses stateful processing to
 * aggregate team scores per team and outputs team name and it's total score every time a team
 * passes a new multiple of a threshold score. For example, multiples of the threshold could be the
 * corresponding scores required to pass each level of the game. By default, this threshold is set
 * to 5000.
 *
 * <p>Stateful processing allows us to write pipelines that output based on a runtime state (when
 * team reaches certain score, in every 100 game events etc) without time triggers. See
 * https://beam.apache.org/blog/2017/02/13/stateful-processing.html for more information on using
 * stateful processing.
 *
 * <p>Run {@code injector.Injector} to generate pubsub data for this pipeline.  The Injector
 * documentation provides more detail on how to do this.
 *
 * <p>To execute this pipeline, specify the pipeline configuration like this:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --tempLocation=gs://YOUR_TEMP_DIRECTORY
 *   --runner=YOUR_RUNNER
 *   --dataset=YOUR-DATASET
 *   --topic=projects/YOUR-PROJECT/topics/YOUR-TOPIC
 * }
 * </pre>
 *
 * <p>The BigQuery dataset you specify must already exist. The PubSub topic you specify should be
 * the same topic to which the Injector is publishing.
 */
public class AggregatedTeamScore extends LeaderBoard {

  /**
   * Options supported by {@link AggregatedTeamScore}.
   */
  interface Options extends LeaderBoard.Options {

    @Description("Numeric value, multiple of which is used as threshold for outputting team score.")
    @Default.Integer(5000)
    Integer getThresholdScore();

    void setThresholdScore(Integer value);
  }

  /**
   * Create a map of information that describes how to write pipeline output to BigQuery. This map
   * is used to write team score sums.
   */
  private static Map<String, FieldInfo<KV<String, Integer>>>
  configureCompleteWindowedTableWrite() {

    Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure =
        new HashMap<String, FieldInfo<KV<String, Integer>>>();
    tableConfigure.put(
        "team",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING", (c, w) -> c.element().getKey()));
    tableConfigure.put(
        "total_score",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
            "INTEGER", (c, w) -> c.element().getValue()));
    tableConfigure.put(
        "processing_time",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING", (c, w) -> fmt.print(Instant.now())));
    return tableConfigure;
  }


  public static void main(String[] args) throws Exception {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true);
    ExampleUtils exampleUtils = new ExampleUtils(options);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        // Read game events from Pub/Sub using custom timestamps, which are extracted from the
        // pubsub data elements, and parse the data.
        .apply(PubsubIO.readStrings()
            .withTimestampAttribute(TIMESTAMP_ATTRIBUTE).fromTopic(options.getTopic()))
        .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))
        // Create <team, GameActionInfo> mapping. UpdateTeamScore uses team name as key.
        .apply("MapTeamAsKey", MapElements
            .into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                TypeDescriptor.of(GameActionInfo.class)))
            .via((GameActionInfo gInfo) -> KV.of(gInfo.team, gInfo)))
        // Outputs team score every time a team passes multiple of the threshold.
        .apply("UpdateTeamScore",
            ParDo.of(new UpdateTeamScoreFn(options.getThresholdScore())))
        // Write the results to BigQuery.
        .apply(
            "WriteTeamLeaders",
            new WriteWindowedToBigQuery<KV<String, Integer>>(
                options.as(GcpOptions.class).getProject(),
                options.getDataset(),
                options.getLeaderBoardTableName() + "_team_leader",
                configureCompleteWindowedTableWrite()));

    // Run the pipeline and wait for the pipeline to finish; capture cancellation requests from the
    // command line.
    PipelineResult result = pipeline.run();
    exampleUtils.waitToFinish(result);
  }

  // Outputs team score every time a team passes a new multiple of the threshold score.
  @VisibleForTesting
  static class UpdateTeamScoreFn
      extends DoFn<KV<String, GameActionInfo>, KV<String, Integer>> {

    private static final String TOTAL_SCORE = "totalScore";
    private final int thresholdScore;

    public UpdateTeamScoreFn(int thresholdScore) {
      this.thresholdScore = thresholdScore;
    }

    @StateId(TOTAL_SCORE)
    private final StateSpec<ValueState<Integer>> totalScoreSpec =
        StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @StateId(TOTAL_SCORE) ValueState<Integer> totalScore) {
      String teamName = c.element().getKey();
      GameActionInfo gInfo = c.element().getValue();

      int oldTotalScore = firstNonNull(totalScore.read(), 0);
      totalScore.write(oldTotalScore + gInfo.score);

      // Since there are no negative scores, the easiest way to check whether a team just passed a
      // new multiple of the threshold score is to compare the quotients of dividing total scores by
      // threshold before and after this aggregation. For example, if the total score was 1999,
      // the new total is 2002, and the threshold is 1000, 1999 / 1000 = 1, 2002 / 1000 = 2.
      // Therefore, this team passed the threshold.
      if (oldTotalScore / this.thresholdScore != totalScore.read() / this.thresholdScore) {
        c.output(KV.of(teamName, totalScore.read()));
      }
    }
  }
}
