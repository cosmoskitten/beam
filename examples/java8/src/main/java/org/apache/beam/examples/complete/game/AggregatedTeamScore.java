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
 * aggregate team scores per team and outputs team name and it's total score after every N points
 * earned. In real world, N could be the total score a team should earn to level up. By default,
 * this threshold (N) is set to 5000.
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

    @Description("Numeric value of fixed window duration for team analysis, in minutes")
    @Default.Integer(5000)
    Integer getThreshold();

    void setThreshold(Integer value);
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
        // Read game events from Pub/Sub using custom timestamps, which are extracted from the pubsub
        // data elements, and parse the data.
        .apply(PubsubIO.readStrings()
            .withTimestampAttribute(TIMESTAMP_ATTRIBUTE).fromTopic(options.getTopic()))
        .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))
        // Create <team, GameActionInfo> mapping. UpdateTeamScore uses team name as key.
        .apply("MapTeamAsKey", MapElements
            .into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                TypeDescriptor.of(GameActionInfo.class)))
            .via((GameActionInfo gInfo) -> KV.of(gInfo.team, gInfo)))
        // Outputs team score every time a team reaches the threshold.
        .apply("UpdateTeamScore",
            ParDo.of(new UpdateTeamScoreFn(options.getThreshold())))
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

  // Outputs team score every time a team reaches the threshold.
  @VisibleForTesting
  static class UpdateTeamScoreFn
      extends DoFn<KV<String, GameActionInfo>, KV<String, Integer>> {

    private final int threshold;

    public UpdateTeamScoreFn(int threshold) {
      this.threshold = threshold;
    }

    @StateId("totalScore")
    private final StateSpec<ValueState<Integer>> totalScoreSpec =
        StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @StateId("totalScore") ValueState<Integer> totalScore) {
      String teamName = c.element().getKey();
      GameActionInfo gInfo = c.element().getValue();

      int oldTotalScore = firstNonNull(totalScore.read(), 0);
      totalScore.write(oldTotalScore + gInfo.score);

      if (oldTotalScore / this.threshold != totalScore.read() / this.threshold) {
        c.output(KV.of(teamName, totalScore.read()));
      }
    }
  }
}
