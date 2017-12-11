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

import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds shards that are open at the given startingPoint.
 */
class StartingPointShardsFinder implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(StartingPointShardsFinder.class);

  Iterable<Shard> findShardsAtStartingPoint(SimplifiedKinesisClient kinesis,
      String streamName, StartingPoint startingPoint)
      throws TransientKinesisException {
    List<Shard> allShards = kinesis.listShards(streamName);
    Set<Shard> initialShards = findInitialShardsWithoutParents(streamName, allShards);

    Set<Shard> startingPointShards = new HashSet<>();
    Sets.SetView<Shard> expiredShards;
    do {
      Set<Shard> validShards = validateShards(kinesis, initialShards, streamName, startingPoint);
      startingPointShards.addAll(validShards);
      expiredShards = Sets.difference(initialShards, validShards);
      Set<String> expiredShardIds = FluentIterable.from(expiredShards)
          .transform(new ShardIdGetter())
          .toSet();
      if (!expiredShardIds.isEmpty()) {
        LOGGER.info("Following shards expired for {} stream at '{}' starting point: {}", streamName,
            startingPoint, expiredShardIds);
      }
      initialShards = FluentIterable.from(allShards)
          .filter(new ExpiredShardsSuccessorsFilter(expiredShardIds))
          .toSet();
    } while (!expiredShards.isEmpty());
    return startingPointShards;
  }

  private Set<Shard> findInitialShardsWithoutParents(String streamName, List<Shard> allShards) {
    final Set<String> shardIds = FluentIterable.from(allShards)
        .transform(new ShardIdGetter())
        .toSet();
    LOGGER.info("Stream {} has following shards: {}", streamName, shardIds);
    return FluentIterable.from(allShards)
        .filter(new ShardsWithoutParentsFilter(shardIds))
        .toSet();
  }

  private Set<Shard> validateShards(SimplifiedKinesisClient kinesis, Iterable<Shard> rootShards,
      String streamName, StartingPoint startingPoint)
      throws TransientKinesisException {
    Set<Shard> validShards = new HashSet<>();
    ShardIteratorType shardIteratorType = ShardIteratorType
        .fromValue(startingPoint.getPositionName());
    for (Shard shard : rootShards) {
      String shardIterator = kinesis.getShardIterator(streamName, shard.getShardId(),
          shardIteratorType, null, startingPoint.getTimestamp());
      GetKinesisRecordsResult records = kinesis.getRecords(shardIterator, streamName,
          shard.getShardId());
      if (records.getNextShardIterator() != null || !records.getRecords().isEmpty()) {
        validShards.add(shard);
      }
    }
    return validShards;
  }

  private static class ShardIdGetter implements Function<Shard, String> {

    @Override
    public String apply(Shard shard) {
      return shard.getShardId();
    }
  }

  private static class ExpiredShardsSuccessorsFilter implements Predicate<Shard> {

    private final Set<String> expiredShardIds;

    ExpiredShardsSuccessorsFilter(Set<String> expiredShardIds) {
      this.expiredShardIds = expiredShardIds;
    }

    @Override
    public boolean apply(Shard shard) {
      return expiredShardIds.contains(shard.getParentShardId());
    }
  }

  private static class ShardsWithoutParentsFilter implements Predicate<Shard> {

    private final Set<String> shardIds;

    ShardsWithoutParentsFilter(Set<String> shardIds) {
      this.shardIds = shardIds;
    }

    @Override
    public boolean apply(Shard shard) {
      return !shardIds.contains(shard.getParentShardId());
    }
  }
}
