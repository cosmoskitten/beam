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
package org.apache.beam.sdk.io.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * Test on RedisPubSubIO.
 */
public class RedisPubSubIOTest {

  private static final Logger LOG = LoggerFactory.getLogger(RedisPubSubIOTest.class);

  @Ignore("Require a Redis server")
  @Test
  @Category(RunnableOnService.class)
  public void testChannelRead() throws Exception {
    LOG.info("Creating publisher");
    final Jedis jedis = new Jedis("localhost", 6379);
    Thread publisher = new Thread() {
      public void run() {
        try {
          Thread.sleep(1000);
          for (int i = 0; i < 1000; i++) {
            jedis.publish("TEST_CHANNEL", "Test " + i);
          }
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    };

    LOG.info("Creating test pipeline");
    TestPipeline pipeline = TestPipeline.create();
    PCollection<String> output = pipeline.apply(RedisPubSubIO.read().withConnectionConfiguration
        (RedisPubSubIO.ConnectionConfiguration.create("localhost", 6379)
            .withChannels(Collections.singletonList("TEST_CHANNEL"))).withMaxNumRecords(1000));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(1000L);

    ArrayList<String> expected = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      expected.add("Test " + i);
    }
    PAssert.that(output).containsInAnyOrder(expected);

    LOG.info("Starting publisher");
    publisher.start();
    LOG.info("Starting pipeline");
    pipeline.run();
    publisher.join();
  }

  @Ignore("Require a Redis server")
  @Test
  @Category(RunnableOnService.class)
  public void testPatternRead() throws Exception {
    LOG.info("Creating publisher");
    final Jedis jedis = new Jedis("localhost", 6379);
    Thread publisher = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(1000);
          for (int i = 0; i < 1000; i++) {
            jedis.publish("TEST_PATTERN", "Test " + i);
          }
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    };

    LOG.info("Creating test pipeline");
    TestPipeline pipeline = TestPipeline.create();
    PCollection<String> output = pipeline.apply(RedisPubSubIO.read().withConnectionConfiguration(
        RedisPubSubIO.ConnectionConfiguration.create("localhost", 6379)
        .withPatterns(Collections.singletonList("TEST_P*"))).withMaxNumRecords(1000));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(1000L);

    ArrayList<String> expected = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      expected.add("Test " + i);
    }
    PAssert.that(output).containsInAnyOrder(expected);

    LOG.info("Starting publisher");
    publisher.start();
    LOG.info("Starting pipeline");
    pipeline.run();
    publisher.join();
  }

  @Ignore("Require a Redis server")
  @Test
  @Category(RunnableOnService.class)
  public void testWrite() throws Exception {
    final ArrayList<String> messages = new ArrayList<>();

    LOG.info("Starting Redis subscriber");
    final Jedis jedis = new Jedis("localhost", 6379);
    Thread subscriber = new Thread() {
      @Override
      public void run() {
        LOG.info("Starting Redis subscriber thread");
        jedis.subscribe(new JedisPubSub() {
          @Override
          public void onMessage(String channel, String message) {
            messages.add(message);
            if (messages.size() == 1000) {
              unsubscribe();
            }
          }
        }, "TEST_WRITE");
      }
    };
    subscriber.start();

    LOG.info("Starting pipeline");
    TestPipeline pipeline = TestPipeline.create();
    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      data.add("Test " + i);
    }
    pipeline.apply(Create.of(data))
        .apply(RedisPubSubIO.write().withConnectionConfiguration(
            RedisPubSubIO.ConnectionConfiguration.create("localhost", 6379)
        .withChannels(Collections.singletonList("TEST_WRITE"))));
    pipeline.run();
    subscriber.join();

    assertEquals(1000, messages.size());
    for (int i = 0; i < 1000; i++) {
      assertTrue(messages.contains("Test " + i));
    }
  }


}
