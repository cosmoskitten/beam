package org.apache.beam.runners.samza.adapter;

import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.TestSamzaRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
//import org.junit.Test;

/**
 * Created by xiliu on 2/5/18.
 */
public class TestTimer {

  //@Test
  public void testTimer() {
    SamzaPipelineOptions options = PipelineOptionsFactory.as(SamzaPipelineOptions.class);
    options.setRunner(TestSamzaRunner.class);
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(KafkaIO.<String, String>read()
          .withTopic("TestTimerTopic")
          .withBootstrapServers("localhost:9092")
          .withKeyDeserializer(StringDeserializer.class)
          .withValueDeserializer(StringDeserializer.class)
          .withoutMetadata())
        .apply(Values.create())
        .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(5)))
            .triggering(AfterProcessingTime.pastFirstElementInPane()
                .plusDelayOf(Duration.standardSeconds(5)))
            .discardingFiredPanes()
            .withAllowedLateness(Duration.standardSeconds(1)))
        .apply(Combine.globally(Count.<String>combineFn()).withoutDefaults())
        .apply(MapElements
            .into(TypeDescriptors.strings())
            .via(count -> {
              String msg = "Count in window is " + count;
              System.out.println(msg);
              return msg;
            }));

    pipeline.run().waitUntilFinish();
  }

}
