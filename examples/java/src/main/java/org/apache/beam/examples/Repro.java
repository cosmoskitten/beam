package org.apache.beam.examples;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by kirpichov on 9/9/17. */
public class Repro {
  private static final Logger LOG = LoggerFactory.getLogger(Repro.class);

  public static void main(String[] args) {
    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setProject("span-cloud-testing");
    options.setTempLocation("gs://mairbek/tmp");
    options.setRunner(DataflowRunner.class);
    options.setStreaming(true);

    Pipeline p = Pipeline.create(options);
    p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)))
        .apply(
            MapElements.into(strings())
                .via(
                    new SerializableFunction<Long, String>() {
                      @Override
                      public String apply(Long input) {
                        LOG.info("Writing message: " + input);
                        return input.toString();
                      }
                    }))
        .apply(
            PubsubIO.writeStrings()
                .to("projects/span-cloud-testing/topics/mairbek-test"));

    p.apply(
        PubsubIO.readStrings()
            .fromTopic("projects/span-cloud-testing/topics/mairbek-test"))
        .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(5))))
        .apply(Combine.globally(Count.<String>combineFn()).withoutDefaults())
        .apply(
            ParDo.of(
                new DoFn<Long, String>() {
                  @ProcessElement
                  public void process(ProcessContext c, BoundedWindow w) {
                    LOG.info("Count for window " + w + " is " + c.element());
                  }
                }));

    p.run().waitUntilFinish();
  }
}
