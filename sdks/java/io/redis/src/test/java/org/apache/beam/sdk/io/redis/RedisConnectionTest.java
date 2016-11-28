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

import static org.junit.Assert.assertFalse;

import java.util.Collections;

import org.junit.Ignore;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;

/**
 * Test on {@link RedisConnectionTest}.
 */
public class RedisConnectionTest {

  @Ignore("Require a single Redis instance")
  @Test
  public void testIsClusterEnabled() throws Exception {
    RedisConnection redisConnection =
        RedisConnection.create(Collections.singletonList("localhost:6379"));
    JedisCommands jedisCommands = redisConnection.connect();
    if (jedisCommands instanceof Jedis) {
      Jedis jedis = (Jedis) jedisCommands;
      assertFalse(redisConnection.isClusterEnabled(jedis));
      return;
    }
    throw new IllegalStateException("JedisCommands is not an instance of Jedis");
  }

  @Ignore("Require a cluster Redis instance")
  @Test
  public void testClusterConnection() throws Exception {
    RedisConnection redisConnection =
        RedisConnection.create(Collections.singletonList("localhost:6379"));
    JedisCommands jedisCommands = redisConnection.connect();
    if (!(jedisCommands instanceof JedisCluster)) {
      throw new IllegalStateException("Not a Redis cluster");
    }
  }

  @Ignore("Require a single Redis instance")
  @Test
  public void testSingleConnection() throws Exception {
    RedisConnection redisConnection =
        RedisConnection.create(Collections.singletonList("localhost:6379"));
    JedisCommands jedisCommands = redisConnection.connect();
    if (!(jedisCommands instanceof Jedis)) {
      throw new IllegalStateException("Not a Redis single instance");
    }
  }

}
