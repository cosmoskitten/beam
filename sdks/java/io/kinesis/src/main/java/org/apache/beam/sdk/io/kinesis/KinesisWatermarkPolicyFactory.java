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
package org.apache.beam.sdk.io.kinesis;

import java.io.Serializable;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Ordering;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Implement this interface to create a {@code KinesisWatermarkPolicy}. Used by the {@code
 * ShardRecordsIterator} to create a watermark policy for every shard.
 */
interface KinesisWatermarkPolicyFactory extends Serializable {

  KinesisWatermarkPolicy createKinesisWatermarkPolicy();

  /** Returns an ArrivalTimeKinesisWatermarkPolicy. */
  static KinesisWatermarkPolicyFactory withArrivalTimePolicy() {
    return ArrivalTimeKinesisWatermarkPolicy::new;
  }

  /**
   * Returns an ArrivalTimeKinesisWatermarkPolicy.
   *
   * @param watermarkIdleDurationThreshold watermark idle duration threshold.
   */
  static KinesisWatermarkPolicyFactory withArrivalTimePolicy(Duration watermarkIdleDurationThreshold) {
    return () -> new ArrivalTimeKinesisWatermarkPolicy(watermarkIdleDurationThreshold);
  }

  /** Returns an ProcessingTimeKinesisWatermarkPolicy. */
  static KinesisWatermarkPolicyFactory withProcessingTimePolicy() {
    return ProcessingTimeKinesisWatermarkPolicy::new;
  }

  /**
   * Returns an custom KinesisWatermarkPolicyFactory.
   *
   * @param watermarkParameters Watermark parameters (timestamp extractor, watermark lag) for the policy.
   */
  static KinesisWatermarkPolicyFactory withCustomWatermarkPolicy(
      WatermarkParameters watermarkParameters) {
    return () -> new CustomWatermarkPolicy(watermarkParameters);
  }

  class ArrivalTimeKinesisWatermarkPolicy implements KinesisWatermarkPolicy {
    private final CustomWatermarkPolicy watermarkPolicy;

    ArrivalTimeKinesisWatermarkPolicy() {
      this.watermarkPolicy =
          new CustomWatermarkPolicy(
              WatermarkParameters.create()
                  .withTimestampFn(KinesisRecord::getApproximateArrivalTimestamp));
    }

    ArrivalTimeKinesisWatermarkPolicy(Duration idleDurationThreshold) {
      WatermarkParameters watermarkParameters =
          WatermarkParameters.create()
              .withTimestampFn(KinesisRecord::getApproximateArrivalTimestamp)
              .withWatermarkIdleDurationThreshold(idleDurationThreshold);
      this.watermarkPolicy = new CustomWatermarkPolicy(watermarkParameters);
    }

    @Override
    public Instant getWatermark() {
      return watermarkPolicy.getWatermark();
    }

    @Override
    public void update(KinesisRecord record) {
      watermarkPolicy.update(record);
    }
  }

  class CustomWatermarkPolicy implements KinesisWatermarkPolicy {
    private WatermarkParameters watermarkParameters;

    CustomWatermarkPolicy(WatermarkParameters watermarkParameters) {
      this.watermarkParameters = watermarkParameters;
    }

    @Override
    public Instant getWatermark() {
      Instant now = Instant.now();
      Instant watermarkIdleThreshold = now.minus(watermarkParameters.getWatermarkIdleDurationThreshold());

      Instant newWatermark = watermarkParameters.getLastUpdateTime().isBefore(watermarkIdleThreshold)
          ? watermarkIdleThreshold
          : watermarkParameters.getEventTime();

      if (newWatermark.isAfter(watermarkParameters.getCurrentWatermark())) {
        watermarkParameters =
            watermarkParameters
                .toBuilder()
                .setCurrentWatermark(newWatermark)
                .build();
      }
      return watermarkParameters.getCurrentWatermark();
    }

    @Override
    public void update(KinesisRecord record) {
      watermarkParameters =
          watermarkParameters
              .toBuilder()
              .setEventTime(
                  Ordering.natural()
                      .max(
                          watermarkParameters.getEventTime(),
                          watermarkParameters.getTimestampFn().apply(record)))
              .setLastUpdateTime(Instant.now())
              .build();
    }
  }

  class ProcessingTimeKinesisWatermarkPolicy implements KinesisWatermarkPolicy {
    private Instant currentWatermark = Instant.now();

    @Override
    public Instant getWatermark() {
      return currentWatermark;
    }

    @Override
    public void update(KinesisRecord record) {
      currentWatermark = Instant.now();
    }
  }
}
