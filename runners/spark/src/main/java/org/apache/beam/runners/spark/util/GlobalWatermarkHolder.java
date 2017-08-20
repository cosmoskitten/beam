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

package org.apache.beam.runners.spark.util;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.spark.SparkEnv;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.BlockResult;
import org.apache.spark.storage.BlockStore;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaBatchInfo;
import org.apache.spark.streaming.api.java.JavaStreamingListener;
import org.apache.spark.streaming.api.java.JavaStreamingListenerBatchCompleted;
import org.apache.spark.streaming.api.java.JavaStreamingListenerBatchStarted;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

/**
 * A {@link BlockStore} variable to hold the global watermarks for a micro-batch.
 *
 * <p>For each source, holds a queue for the watermarks of each micro-batch that was read,
 * and advances the watermarks according to the queue (first-in-first-out).
 */
public class GlobalWatermarkHolder {
  private static final Map<Integer, Queue<SparkWatermarks>> sourceTimes = new HashMap<>();
  private static final BlockId WATERMARKS_BLOCK_ID = BlockId.apply("broadcast_0WATERMARKS");

  private static volatile Map<Integer, SparkWatermarks> driverWatermarks = null;
  private static volatile LoadingCache<String, Map<Integer, SparkWatermarks>> watermarkCache = null;

  private static volatile long lastWatermarkedBatchTime = 0;
  private static volatile Long inFlightBatchTime = null;

  public static long getLastWatermarkedBatchTime() {
    return lastWatermarkedBatchTime;
  }

  public static Long getInFlightBatchTime() {
    return inFlightBatchTime;
  }

  public static void add(int sourceId, SparkWatermarks sparkWatermarks) {
    Queue<SparkWatermarks> timesQueue = sourceTimes.get(sourceId);
    if (timesQueue == null) {
      timesQueue = new ConcurrentLinkedQueue<>();
    }
    timesQueue.offer(sparkWatermarks);
    sourceTimes.put(sourceId, timesQueue);
  }

  @VisibleForTesting
  public static void addAll(Map<Integer, Queue<SparkWatermarks>> sourceTimes) {
    for (Map.Entry<Integer, Queue<SparkWatermarks>> en: sourceTimes.entrySet()) {
      int sourceId = en.getKey();
      Queue<SparkWatermarks> timesQueue = en.getValue();
      while (!timesQueue.isEmpty()) {
        add(sourceId, timesQueue.poll());
      }
    }
  }

  /**
   * Returns the {@link Broadcast} containing the {@link SparkWatermarks} mapped
   * to their sources.
   */
  @SuppressWarnings("unchecked")
  public static Map<Integer, SparkWatermarks> get(Long cacheInterval) {
    if (driverWatermarks != null) {
      // if we are executing in local mode simply return the local values.
      return driverWatermarks;
    } else {
      if (watermarkCache == null) {
        initWatermarkCache(cacheInterval);
      }
      try {
        return watermarkCache.get("SINGLETON");
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static synchronized void initWatermarkCache(Long batchDuration) {
    if (watermarkCache == null) {
      watermarkCache =
          CacheBuilder.newBuilder()
              // expire watermarks every half batch duration to ensure they update in every batch.
              .expireAfterWrite(batchDuration / 2, TimeUnit.MILLISECONDS)
              .build(new WatermarksLoader());
    }
  }

  /**
   * Advances the watermarks to the next-in-line watermarks.
   * SparkWatermarks are monotonically increasing.
   */
  @SuppressWarnings("unchecked")
  public static void advance() {
    synchronized (GlobalWatermarkHolder.class) {
      BlockManager blockManager = SparkEnv.get().blockManager();

      if (sourceTimes.isEmpty()) {
        return;
      }

      // update all sources' watermarks into the new broadcast.
      Map<Integer, SparkWatermarks> newValues = new HashMap<>();

      for (Map.Entry<Integer, Queue<SparkWatermarks>> watermarkInfo: sourceTimes.entrySet()) {
        if (watermarkInfo.getValue().isEmpty()) {
          continue;
        }
        Integer sourceId = watermarkInfo.getKey();

        // current state, if exists.
        Instant currentLowWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
        Instant currentHighWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
        Instant currentSynchronizedProcessingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;

        Map<Integer, SparkWatermarks> currentWatermarks = initWatermarks(blockManager);

        if (currentWatermarks.containsKey(sourceId)) {
          SparkWatermarks currentTimes = currentWatermarks.get(sourceId);
          currentLowWatermark = currentTimes.getLowWatermark();
          currentHighWatermark = currentTimes.getHighWatermark();
          currentSynchronizedProcessingTime = currentTimes.getSynchronizedProcessingTime();
        }

        Queue<SparkWatermarks> timesQueue = watermarkInfo.getValue();
        SparkWatermarks next = timesQueue.poll();
        // advance watermarks monotonically.
        Instant nextLowWatermark = next.getLowWatermark().isAfter(currentLowWatermark)
            ? next.getLowWatermark() : currentLowWatermark;
        Instant nextHighWatermark = next.getHighWatermark().isAfter(currentHighWatermark)
            ? next.getHighWatermark() : currentHighWatermark;
        Instant nextSynchronizedProcessingTime = next.getSynchronizedProcessingTime();
        checkState(!nextLowWatermark.isAfter(nextHighWatermark),
            String.format(
                "Low watermark %s cannot be later then high watermark %s",
                nextLowWatermark, nextHighWatermark));
        checkState(nextSynchronizedProcessingTime.isAfter(currentSynchronizedProcessingTime),
            "Synchronized processing time must advance.");
        newValues.put(
            sourceId,
            new SparkWatermarks(
                nextLowWatermark, nextHighWatermark, nextSynchronizedProcessingTime));
      }

      // update the watermarks broadcast only if something has changed.
      updateWatermarks(blockManager, newValues);
    }
  }

  private static void updateWatermarks(BlockManager blockManager,
                                       Map<Integer, SparkWatermarks> newValues) {
    if (!newValues.isEmpty()) {
      driverWatermarks = newValues;
      blockManager.removeBlock(WATERMARKS_BLOCK_ID, true);
      blockManager.putSingle(
          WATERMARKS_BLOCK_ID,
          newValues,
          StorageLevel.MEMORY_ONLY(),
          true);
    }
  }

  private static Map<Integer, SparkWatermarks> initWatermarks(final BlockManager blockManager) {

    Map<Integer, SparkWatermarks> watermarks = fetchSparkWatermarks(blockManager);

    if (watermarks == null) {
      HashMap<Integer, SparkWatermarks> empty = Maps.newHashMap();
      blockManager.putSingle(
          WATERMARKS_BLOCK_ID,
          empty,
          StorageLevel.MEMORY_ONLY(),
          true);
      return empty;
    } else {
      return watermarks;
    }
  }

  @VisibleForTesting
  public static synchronized void clear() {
    sourceTimes.clear();
    driverWatermarks = null;
    inFlightBatchTime = null;
    lastWatermarkedBatchTime = 0;
    SparkEnv sparkEnv = SparkEnv.get();
    if (sparkEnv != null) {
      BlockManager blockManager = sparkEnv.blockManager();
      blockManager.removeBlock(WATERMARKS_BLOCK_ID, true);
    }
  }

  /**
   * A {@link SparkWatermarks} holds the watermarks and batch time
   * relevant to a micro-batch input from a specific source.
   */
  public static class SparkWatermarks implements Serializable {
    private final Instant lowWatermark;
    private final Instant highWatermark;
    private final Instant synchronizedProcessingTime;

    @VisibleForTesting
    public SparkWatermarks(
        Instant lowWatermark,
        Instant highWatermark,
        Instant synchronizedProcessingTime) {
      this.lowWatermark = lowWatermark;
      this.highWatermark = highWatermark;
      this.synchronizedProcessingTime = synchronizedProcessingTime;
    }

    public Instant getLowWatermark() {
      return lowWatermark;
    }

    public Instant getHighWatermark() {
      return highWatermark;
    }

    public Instant getSynchronizedProcessingTime() {
      return synchronizedProcessingTime;
    }

    @Override
    public String toString() {
      return "SparkWatermarks{"
          + "lowWatermark=" + lowWatermark
          + ", highWatermark=" + highWatermark
          + ", synchronizedProcessingTime=" + synchronizedProcessingTime + '}';
    }
  }

  /** Advance the WMs onBatchCompleted event. */
  public static class WatermarksListener extends JavaStreamingListener {
    private static final Logger LOG = LoggerFactory.getLogger(WatermarksListener.class);

    private long timeOf(JavaBatchInfo info) {
      return info.batchTime().milliseconds();
    }

    private long laterOf(long t1, long t2) {
      return Math.max(t1, t2);
    }

    @Override
    public void onBatchStarted(JavaStreamingListenerBatchStarted batchStarted) {
      inFlightBatchTime = laterOf(Optional.fromNullable(inFlightBatchTime).or(0L),
                                  timeOf(batchStarted.batchInfo()));
    }

    @Override
    public void onBatchCompleted(JavaStreamingListenerBatchCompleted batchCompleted) {
      lastWatermarkedBatchTime =
          laterOf(lastWatermarkedBatchTime, timeOf(batchCompleted.batchInfo()));

      GlobalWatermarkHolder.advance();

      LOG.info("Batch with timestamp: {} has completed, watermarks have been updated.",
               lastWatermarkedBatchTime);
    }
  }

  private static Map<Integer, SparkWatermarks> fetchSparkWatermarks(BlockManager blockManager) {
    Option<BlockResult> blockResultOption = blockManager.getRemote(WATERMARKS_BLOCK_ID);
    if (blockResultOption.isDefined()) {
      return (Map<Integer, SparkWatermarks>) blockResultOption.get().data().next();
    } else {
      return null;
    }
  }

  private static class WatermarksLoader extends CacheLoader<String, Map<Integer, SparkWatermarks>> {

    @SuppressWarnings("unchecked")
    @Override
    public Map<Integer, SparkWatermarks> load(@Nonnull String key) throws Exception {
      Map<Integer, SparkWatermarks> watermarks =
          fetchSparkWatermarks(SparkEnv.get().blockManager());
      return watermarks != null ? watermarks : Maps.<Integer, SparkWatermarks>newHashMap();
    }
  }
}
