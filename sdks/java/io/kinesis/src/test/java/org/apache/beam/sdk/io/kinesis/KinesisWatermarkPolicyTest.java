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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

/** Tests {@link KinesisWatermarkPolicy}. */
@RunWith(MockitoJUnitRunner.class)
public class KinesisWatermarkPolicyTest {
  private static final String SHARD_1 = "shard1";
  private static final String SHARD_2 = "shard2";
  private static final Instant NOW = Instant.now();

  @Mock private ShardRecordsIterator firstIterator, secondIterator;
  @Mock private ShardCheckpoint firstCheckpoint, secondCheckpoint;
  @Mock private SimplifiedKinesisClient kinesis;
  @Mock private KinesisWatermarkPolicyFactory factory;

  private ShardReadersPool shardReadersPool;

  @Before
  public void setUp() throws TransientKinesisException {
    when(firstCheckpoint.getShardId()).thenReturn(SHARD_1);
    when(secondCheckpoint.getShardId()).thenReturn(SHARD_2);

    when(firstIterator.getShardId()).thenReturn(SHARD_1);
    when(secondIterator.getShardId()).thenReturn(SHARD_2);
    when(firstIterator.getCheckpoint()).thenReturn(firstCheckpoint);
    when(secondIterator.getCheckpoint()).thenReturn(secondCheckpoint);
    KinesisReaderCheckpoint checkpoint =
        new KinesisReaderCheckpoint(ImmutableList.of(firstCheckpoint, secondCheckpoint));

    shardReadersPool = Mockito.spy(new ShardReadersPool(kinesis, checkpoint, factory));

    doReturn(firstIterator).when(shardReadersPool).createShardIterator(kinesis, firstCheckpoint);
    doReturn(secondIterator).when(shardReadersPool).createShardIterator(kinesis, secondCheckpoint);
  }

  @After
  public void clean() {
    shardReadersPool.stop();
  }

  @Test
  public void shouldAdvanceWatermarkWithTheArrivalTimeFromKinesisRecords() {
    KinesisWatermarkPolicy policy =
        KinesisWatermarkPolicyFactory.withArrivalTimePolicy().createKinesisWatermarkPolicy();

    KinesisRecord a = mock(KinesisRecord.class);
    KinesisRecord b = mock(KinesisRecord.class);

    Instant time1 = NOW.minus(Duration.standardSeconds(30L));
    Instant time2 = NOW.minus(Duration.standardSeconds(20L));
    when(a.getApproximateArrivalTimestamp()).thenReturn(time1);
    when(b.getApproximateArrivalTimestamp()).thenReturn(time2);

    policy.update(a);
    assertThat(policy.getWatermark()).isEqualTo(time1);
    policy.update(b);
    assertThat(policy.getWatermark()).isEqualTo(time2);
  }

  @Test
  public void shouldOnlyAdvanceTheWatermark() {
    KinesisWatermarkPolicy policy =
        KinesisWatermarkPolicyFactory.withArrivalTimePolicy().createKinesisWatermarkPolicy();

    KinesisRecord a = mock(KinesisRecord.class);
    KinesisRecord b = mock(KinesisRecord.class);
    KinesisRecord c = mock(KinesisRecord.class);

    Instant time1 = NOW.minus(Duration.standardSeconds(30L));
    Instant time2 = NOW.minus(Duration.standardSeconds(20L));
    Instant time3 = NOW.minus(Duration.standardSeconds(40L));
    when(a.getApproximateArrivalTimestamp()).thenReturn(time1);
    when(b.getApproximateArrivalTimestamp()).thenReturn(time2);
    when(c.getApproximateArrivalTimestamp()).thenReturn(time3);

    policy.update(a);
    assertThat(policy.getWatermark()).isEqualTo(time1);
    policy.update(b);
    assertThat(policy.getWatermark()).isEqualTo(time2);
    policy.update(c);
    // watermark doesn't go back in time
    assertThat(policy.getWatermark()).isEqualTo(time2);
  }

  @Test
  public void shouldAdvanceWatermarkWhenThereAreNoIncomingRecords() {
    KinesisWatermarkPolicy policy =
        KinesisWatermarkPolicyFactory.withArrivalTimePolicy().createKinesisWatermarkPolicy();

    Instant time = NOW.minus(Duration.standardMinutes(2));

    // the watermark should advance even if there are no records
    // The expected watermark will be NOW - threshold (2 minutes by default)
    assertThat(policy.getWatermark()).isBetween(time, time.plus(Duration.standardSeconds(10)));
  }

  @Test
  public void shouldAdvanceWatermarkWithCustomWatermarkLagWhenThereAreNoIncomingRecords() {
    KinesisWatermarkPolicy policy =
        KinesisWatermarkPolicyFactory.withCustomWatermarkPolicy(
                WatermarkParameters.create().withWatermarkLagThreshold(Duration.standardHours(1)))
            .createKinesisWatermarkPolicy();

    Instant time = NOW.minus(Duration.standardHours(1));

    // the watermark should advance even if there are no records
    // The expected watermark will be NOW - 1 hr custom threshold
    assertThat(policy.getWatermark()).isBetween(time, time.plus(Duration.standardSeconds(10)));
  }

  @Test
  public void shouldAdvanceWatermarkToNowWithProcessingTimePolicy() {
    KinesisWatermarkPolicy policy =
        KinesisWatermarkPolicyFactory.withProcessingTimePolicy().createKinesisWatermarkPolicy();

    KinesisRecord a = mock(KinesisRecord.class);
    KinesisRecord b = mock(KinesisRecord.class);

    policy.update(a);
    assertThat(policy.getWatermark()).isGreaterThan(NOW);
    policy.update(b);
    assertThat(policy.getWatermark()).isGreaterThan(NOW);
  }

  @Test
  public void shouldAdvanceWatermarkWithCustomTimePolicy() {
    SerializableFunction<KinesisRecord, Instant> timestampFn =
        (record) -> record.getApproximateArrivalTimestamp().plus(Duration.standardMinutes(1));

    KinesisWatermarkPolicy policy =
        KinesisWatermarkPolicyFactory.withCustomWatermarkPolicy(
                WatermarkParameters.create().withTimestampFn(timestampFn))
            .createKinesisWatermarkPolicy();

    KinesisRecord a = mock(KinesisRecord.class);
    KinesisRecord b = mock(KinesisRecord.class);

    Instant time1 = NOW.minus(Duration.standardSeconds(30L));
    Instant time2 = NOW.minus(Duration.standardSeconds(20L));
    when(a.getApproximateArrivalTimestamp()).thenReturn(time1);
    when(b.getApproximateArrivalTimestamp()).thenReturn(time2);

    policy.update(a);
    assertThat(policy.getWatermark()).isEqualTo(time1.plus(Duration.standardMinutes(1)));
    policy.update(b);
    assertThat(policy.getWatermark()).isEqualTo(time2.plus(Duration.standardMinutes(1)));
  }
}
