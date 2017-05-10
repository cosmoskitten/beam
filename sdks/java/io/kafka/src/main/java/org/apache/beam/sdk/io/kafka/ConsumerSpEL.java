/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.kafka;

import com.google.common.collect.Iterables;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * ConsumerSpEL to handle multiple of versions of Consumer API between Kafka 0.9 and 0.10.
 * It auto detects the input type List/Collection/Varargs,
 * to eliminate the method definition differences.
 */
class ConsumerSpEL {
  private static final Logger LOG = LoggerFactory.getLogger(ConsumerSpEL.class);

  private SpelParserConfiguration config = new SpelParserConfiguration(true, true);
  private ExpressionParser parser = new SpelExpressionParser(config);

  private Expression seek2endExpression =
      parser.parseExpression("#consumer.seekToEnd(#tp)");

  private Expression assignExpression =
      parser.parseExpression("#consumer.assign(#tp)");

  private Method timestampMethod;
  private boolean hasRecordTimestamp = false;

  private Method offsetsForTimesMethod;
  private Method endOffsetsMethod;
  private Method offsetMethod;
  private boolean hasOffsetsForTimes = false;

  public ConsumerSpEL() {
    try {
      timestampMethod = ConsumerRecord.class.getMethod("timestamp", (Class<?>[]) null);
      hasRecordTimestamp = timestampMethod.getReturnType().equals(Long.TYPE);
    } catch (NoSuchMethodException | SecurityException e) {
      LOG.debug("Timestamp for Kafka message is not available.");
    }

    try {
      offsetsForTimesMethod = KafkaConsumer.class.getMethod("offsetsForTimes", Map.class);
      hasOffsetsForTimes = offsetsForTimesMethod.getReturnType().equals(Map.class);
      endOffsetsMethod = KafkaConsumer.class.getMethod("endOffsets", Collection.class);
      offsetMethod = Class.forName("org.apache.kafka.clients.consumer.OffsetAndTimestamp")
          .getMethod("offset", (Class<?>[]) null);
    } catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
      LOG.debug("OffsetsForTimes is not available.");
    }
  }

  public void evaluateSeek2End(Consumer consumer, TopicPartition topicPartitions) {
    StandardEvaluationContext mapContext = new StandardEvaluationContext();
    mapContext.setVariable("consumer", consumer);
    mapContext.setVariable("tp", topicPartitions);
    seek2endExpression.getValue(mapContext);
  }

  public void evaluateAssign(Consumer consumer, Collection<TopicPartition> topicPartitions) {
    StandardEvaluationContext mapContext = new StandardEvaluationContext();
    mapContext.setVariable("consumer", consumer);
    mapContext.setVariable("tp", topicPartitions);
    assignExpression.getValue(mapContext);
  }

  public long getRecordTimestamp(ConsumerRecord<byte[], byte[]> rawRecord) {
    long timestamp;
    try {
      //for Kafka 0.9, set to System.currentTimeMillis();
      //for kafka 0.10, when NO_TIMESTAMP also set to System.currentTimeMillis();
      if (!hasRecordTimestamp || (timestamp = (long) timestampMethod.invoke(rawRecord)) <= 0L) {
        timestamp = System.currentTimeMillis();
      }
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      // Not expected. Method timestamp() is already checked.
      throw new RuntimeException(e);
    }
    return timestamp;
  }

  @SuppressWarnings("unchecked")
  public long offsetForTime(Consumer<?, ?> consumer, TopicPartition topicPartition, Date time) {
    if (hasOffsetsForTimes && time != null) {
      Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
      timestampsToSearch.put(topicPartition, time.getTime());
      try {
        Map offsetsByTimes = (Map) offsetsForTimesMethod.invoke(consumer, timestampsToSearch);
        Object offsetAndTimestamp = Iterables.getOnlyElement(offsetsByTimes.values());

        // 1. If no message has a timestamp that is greater than or equals to the target time,
        //    a null will be returned.
        // 2. If the message format version in a partition is before 0.10.0, i.e.
        //    the messages do not have timestamps, null will be returned for that partition.
        if (offsetAndTimestamp == null) {
          Map<TopicPartition, Long> endOffsets =
              (Map<TopicPartition, Long>) endOffsetsMethod.invoke(
                  consumer, Collections.singleton(topicPartition));
          return Iterables.getOnlyElement(endOffsets.values());
        } else {
          return (long) offsetMethod.invoke(offsetAndTimestamp);
        }
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    } else {
      return consumer.position(topicPartition);
    }
  }

}
