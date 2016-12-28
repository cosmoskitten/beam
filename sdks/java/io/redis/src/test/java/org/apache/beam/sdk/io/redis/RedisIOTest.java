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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

/**
 * Test on the Redis IO.
 */
public class RedisIOTest {

  private static final Logger LOG = LoggerFactory.getLogger(RedisIOTest.class);

  private static final List<String> DEFAULT_REDIS = Collections.singletonList("localhost:6379");

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Ignore("Require a Redis server")
  @Test
  public void testEstimatedSizeBytes() throws Exception {
    Jedis jedis = new Jedis("localhost", 6379);
    for (int i = 0; i < 10; i++) {
      jedis.set("Foo " + i, "Bar " + i);
    }
    jedis.quit();

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    RedisIO.Read read = RedisIO.read().withConnection(RedisConnection.create(DEFAULT_REDIS));
    RedisIO.BoundedRedisSource source = new RedisIO.BoundedRedisSource(read);
    long estimatedSizeBytes = source.getEstimatedSizeBytes(pipelineOptions);
    LOG.info("Estimated size: {}", estimatedSizeBytes);
    assertEquals(100, estimatedSizeBytes);
  }

  @Ignore("Require a Redis server")
  @Test
  @Category(RunnableOnService.class)
  public void testGetRead() throws Exception {
    Jedis jedis = new Jedis("localhost", 6379);
    for (int i = 0; i < 1000; i++) {
      jedis.set("Key " + i, "Value " + i);
    }

    PCollection<KV<String, String>> output =
        pipeline.apply(RedisIO.read().withConnection(RedisConnection.create(DEFAULT_REDIS)));

    PAssert.thatSingleton(output.apply("Count", Count.<KV<String, String>>globally()))
        .isEqualTo(1000L);
    ArrayList<KV<String, String>> expected = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      KV<String, String> kv = KV.of("Key " + i, "Value " + i);
      expected.add(kv);
    }
    PAssert.that(output).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Ignore("Require a Redis server")
  @Test
  @Category(RunnableOnService.class)
  public void testSetWrite() throws Exception {
    ArrayList<KV<String, String>> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      KV<String, String> kv = KV.of("Key " + i, "Value " + i);
      data.add(kv);
    }
    pipeline.apply(Create.of(data))
        .apply(RedisIO.write().withConnection(RedisConnection.create(DEFAULT_REDIS)));
    pipeline.run();

    Jedis jedis = new Jedis("localhost", 6379);
    for (int i = 0; i < 1000; i++) {
      String value = jedis.get("Key " + i);
      assertEquals("Value " + i, value);
    }
  }

  @Ignore("Require a Redis server")
  @Test
  @Category(RunnableOnService.class)
  public void testAppendWrite() throws Exception {
    Jedis jedis = new Jedis("localhost", 6379);
    for (int i = 0; i < 1000; i++) {
      jedis.set("Key " + i, "Value " + i);
    }

    ArrayList<KV<String, String>> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      KV<String, String> kv = KV.of("Key " + i, " Appended");
      data.add(kv);
    }
    pipeline.apply(Create.of(data))
        .apply(RedisIO.write().withConnection(RedisConnection.create(DEFAULT_REDIS))
            .withCommand(RedisIO.Write.Command.APPEND));
    pipeline.run();

    for (int i = 0; i < 1000; i++) {
      String value = jedis.get("Key " + i);
      assertEquals("Value " + i +  " Appended", value);
    }
  }

}
