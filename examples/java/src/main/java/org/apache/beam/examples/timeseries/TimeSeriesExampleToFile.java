/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.examples.timeseries;

import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.timeseries.FileSinkTimeSeriesOptions;
import org.apache.beam.sdk.extensions.timeseries.configuration.TSConfiguration;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData;
import org.apache.beam.sdk.extensions.timeseries.protos.TimeSeriesData.TSAccum;
import org.apache.beam.sdk.extensions.timeseries.transforms.*;
import org.apache.beam.sdk.extensions.timeseries.utils.TSAccumSequences;
import org.apache.beam.sdk.extensions.timeseries.utils.TSAccums;
import org.apache.beam.sdk.extensions.timeseries.utils.TSDatas;
import org.apache.beam.sdk.extensions.timeseries.utils.TSMultiVariateDataPoints;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * This example pipeline is used to illustrate an advanced use of Keyed state and timers. The
 * pipeline extracts interesting information from timeseries data. One of the key elements, is the
 * transfer of data between fixed windows for a given key, as well as backfill when a key does not
 * have any new data within a time boundary. This sample should not be used in production.
 *
 * <p>The generated data is 5 identical waves with keys {Sin-1...Sin-5}, each data point has two
 * values {x,y}. x increments by 1 starting from 0 with a missed value in the series. { 0 .. 19 } -
 * {10} y is a simple f(x). The timestamp of the elements starts at 2018-01-01T00:00Z and increments
 * one sec for each x.
 *
 * <p>The output of this pipeline is to a File path, Using the format defined in
 * FeatureDirectoryFileNaming. Even though the aggregations for the x values are are of little value
 * as output, it is processed as part of this demo as it is useful when eyeballing the results.
 */
public class TimeSeriesExampleToFile {

  public static void main(String[] args) {

    // Create pipeline
    FileSinkTimeSeriesOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FileSinkTimeSeriesOptions.class);

    options.setDownSampleDurationMillis(1000L);
    options.setTimeToLiveMillis(60000L);
    options.setFillOption(TSConfiguration.BFillOptions.LAST_KNOWN_VALUE.name());
    options.setForwardPadSequencedOutput(true);
    options.setBackPadSequencedOutput(true);

    Pipeline p = Pipeline.create(options);

    // ------------ READ DATA ------------

    // Read some dummy timeseries data
    PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSDataPoint>> readL1Data =
        p.apply(Create.of(SinWaveSample.generateSinWave()))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ExtractTimeStamp()))
            .apply(ParDo.of(new TSMultiVariateDataPoints.ConvertMultiToUniDataPoint()));

    // ------------ Create perfect rectangles of data--------

    PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> downSampled =
        readL1Data.apply(new ExtractAggregates());

    PCollection<KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>> weHaveOrder =
        downSampled.apply(new OrderOutput());

    // ------------ OutPut Data as Logs in TFRecord format --------

    // This transform is purely to allow logged debug output, it will fail with OOM if a large dataset is used.
    weHaveOrder.apply(new DebugSortedResult());

    // Output raw values
    weHaveOrder.apply(
        FileIO.<String, KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>>writeDynamic()
            .by(x -> TSAccums.getTSAccumKeyMillsTimeBoundary(x.getValue()))
            .withDestinationCoder(StringUtf8Coder.of())
            .withNaming(FeatureDirectoryFileNaming::new)
            .via(
                Contextful.fn(new TimeSeriesExampleToFile.TSAccumToExampleByteFn()),
                TFRecordIO.sink())
            .to(options.getFileSinkDirectory() + "/tf/raw/tfExamplerecords"));

    weHaveOrder
        .apply(new TSAccumToFixedWindowSeq(Duration.standardMinutes(1)))
        .apply(Values.create())
        .apply(
            FileIO.<String, TimeSeriesData.TSAccumSequence>writeDynamic()
                .by(x -> TSAccumSequences.getTSAccumSequenceKeyMillsTimeBoundary(x))
                .withDestinationCoder(StringUtf8Coder.of())
                .withNaming(FeatureDirectoryFileNaming::new)
                .via(
                    Contextful.fn(new TimeSeriesExampleToFile.TSAccumSequenceToExampleByteFn()),
                    TFRecordIO.sink())
                .to(options.getFileSinkDirectory() + "/tf/1min/tfSequence"));

    p.run();
  }

  /** Simple data generator that creates some dummy test data for the timeseries examples. */
  private static class SinWaveSample {

    private static List<TimeSeriesData.TSMultiVariateDataPoint> generateSinWave() {

      double y;
      double yBase = 1;
      double scale = 20;

      List<TimeSeriesData.TSMultiVariateDataPoint> dataPoints = new ArrayList<>();

      for (int k = 0; k < 1; k++) {

        Instant now = Instant.parse("2018-01-01T00:00Z");

        for (int i = 0; i < 120; i++) {

          if (!(i % 10 == 0)) {

            Instant dataPointTimeStamp = now.plus(Duration.standardSeconds(i));

            y = (yBase - Math.sin(Math.toRadians(i)) * scale);

            TimeSeriesData.TSMultiVariateDataPoint mvts =
                TimeSeriesData.TSMultiVariateDataPoint.newBuilder()
                    .setKey(TimeSeriesData.TSKey.newBuilder().setMajorKey("Sin-" + k).build())
                    .putData("x", TSDatas.createData(i))
                    .putData("y", TSDatas.createData(y))
                    .setTimestamp(Timestamps.fromMillis(dataPointTimeStamp.getMillis()))
                    .build();

            dataPoints.add(mvts);
          }
        }
      }

      return dataPoints;
    }
  }

  static class TSAccumToExampleByteFn
      implements SerializableFunction<KV<TimeSeriesData.TSKey, TSAccum>, byte[]> {

    @Override
    public byte[] apply(KV<TimeSeriesData.TSKey, TSAccum> element) {
      byte[] returnVal;
      try {
        returnVal = TSAccums.getExampleFromAccum(element.getValue()).toByteArray();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return returnVal;
    }
  }

  static class TSAccumSequenceToExampleByteFn
      implements SerializableFunction<TimeSeriesData.TSAccumSequence, byte[]> {

    @Override
    public byte[] apply(TimeSeriesData.TSAccumSequence element) {
      byte[] returnVal;
      try {
        returnVal = TSAccumSequences.getSequenceExampleFromAccumSequence(element).toByteArray();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return returnVal;
    }
  }

  static class FeatureDirectoryFileNaming implements FileIO.Write.FileNaming {

    String partitionValue;

    public FeatureDirectoryFileNaming(String partitionValue) {
      this.partitionValue = partitionValue;
    }

    @Override
    public String getFilename(
        BoundedWindow window,
        PaneInfo pane,
        int numShards,
        int shardIndex,
        Compression compression) {
      return String.format(
          "%s/%s-shard-%s-of-%s", this.partitionValue, this.partitionValue, shardIndex, numShards);
    }
  }
}
