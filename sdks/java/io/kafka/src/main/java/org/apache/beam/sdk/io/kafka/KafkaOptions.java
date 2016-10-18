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
package org.apache.beam.sdk.io.kafka;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Duration;



/**
 * Properties for {@link org.apache.beam.sdk.io.kafka.KafkaIO.UnboundedKafkaReader}.
 */
public interface KafkaOptions extends PipelineOptions {

  @Description("How long to wait for new records from kafka consumer reading for the first time.")
  @Default.InstanceFactory(DefaultStartPollTimeDuration.class)
  Duration getStartPollTimeout();
  void setStartPollTimeout(Duration startPollTimeout);

  /**
   * Default {@link Duration} for
   * {@link org.apache.beam.sdk.io.kafka.KafkaIO.UnboundedKafkaReader#startNewRecordsPollTimeout}.
   */
  static class DefaultStartPollTimeDuration implements DefaultValueFactory<Duration> {
    @Override
    public Duration create(PipelineOptions options) {
      return Duration.standardSeconds(5);
    }
  }

  @Description("How long to wait for new records from kafka consumer in-between fetching.")
  @Default.InstanceFactory(DefaultAdvancePollTimeDuration.class)
  Duration getAdvancePollTimeout();
  void setAdvancePollTimeout(Duration advancePollTimeout);

  /**
   * Default {@link Duration} for
   * {@link org.apache.beam.sdk.io.kafka.KafkaIO.UnboundedKafkaReader#newRecordsPollTimeout}.
   */
  static class DefaultAdvancePollTimeDuration implements DefaultValueFactory<Duration> {
    @Override
    public Duration create(PipelineOptions options) {
      return Duration.millis(10);
    }
  }
}
