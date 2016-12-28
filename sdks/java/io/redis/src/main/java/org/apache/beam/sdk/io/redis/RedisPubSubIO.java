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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * An IO to use Redis PubSub.
 *
 * <h3>Reading from a Redis channel</h3>
 *
 * <p>RedisPubSubIO source returns an unbounded {@link PCollection} containing messages (as
 * {@code String}).
 *
 * <p>To configure a Redis PubSub source, you have to provide a Redis connection configuration
 * with a list of {@code host:port} to connect to the Redis server,
 * and your choice of channels or patterns where to subscribe. The following example illustrates
 * various options for configuring the source:
 *
 * <pre>{@code
 *
 *  pipeline.apply(
 *    RedisPubSubIO.read()
 *      .withConnection(RedisConnection.create(Collections.singletonList("localhost:6379")))
 *      .withChannels(Collections.singletonList("CHANNEL"))
 *
 * }</pre>
 *
 * <h3>Writing to a Redis channel</h3>
 *
 * <p>RedisPubSubIO sink supports writing {@code String} to a Redis channel.
 *
 * <p>To configure a Redis PubSub sink, as for the read, you have to specify a Redis connection
 * configuration with {@code host:port}. You can also provide the list of channels where to
 * publish messages.
 *
 * <p>For instance:
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(...) // provide PCollection<String>
 *    .apply(RedisPubSubIO.write()
 *      .withConnection(RedisConnection.create(Collections.singletonList("localhost:6379")))
 *      .withChannels(Collections.singletonList("CHANNEL"));
 *
 * }</pre>
 */
public class RedisPubSubIO {

  private static final Logger LOG = LoggerFactory.getLogger(RedisPubSubIO.class);

  public static Read read() {
    return new AutoValue_RedisPubSubIO_Read.Builder()
        .setMaxReadTime(null).setMaxNumRecords(Long.MAX_VALUE).build();
  }

  public static Write write() {
    return new AutoValue_RedisPubSubIO_Write.Builder().build();
  }

  private RedisPubSubIO() {
  }

  /**
   * A {@link PTransform} to read from Redis PubSub.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    @Nullable abstract RedisConnection connection();
    @Nullable abstract List<String> channels();
    @Nullable abstract List<String> patterns();
    abstract long maxNumRecords();
    @Nullable abstract Duration maxReadTime();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnection(RedisConnection connection);
      abstract Builder setChannels(List<String> channels);
      abstract Builder setPatterns(List<String> patterns);
      abstract Builder setMaxNumRecords(long maxNumRecords);
      abstract Builder setMaxReadTime(Duration maxReadTime);
      abstract Read build();
    }

    /**
     * Define the connection to the Redis PubSub broker.
     *
     * @param connection The {@link RedisConnection} .
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withConnection(RedisConnection connection) {
      checkArgument(connection != null, "RedisPubSubIO.read().withConnection"
          + "(connection) called with null connection");
      return builder().setConnection(connection).build();
    }

    /**
     * Define the list of Redis channels where the source subscribes.
     *
     * @param channels The list of channels.
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withChannels(List<String> channels) {
      checkArgument(channels != null, "RedisPubSubIO.read().withChannels(channels) called with "
          + "null channels");
      checkArgument(!channels.isEmpty(), "RedisPubSubIO.read().withChannels(channels) called "
          + "with empty channels list");
      return builder().setChannels(channels).build();
    }

    public Read withPatterns(List<String> patterns) {
      checkArgument(patterns != null, "RedisPubSubIO.read().withPatterns(patterns) called with "
          + "null patterns");
      checkArgument(!patterns.isEmpty(), "RedisPubSubIO.read().withPatterns(patterns) called "
          + "with empty patterns list");
      return builder().setPatterns(patterns).build();
    }

    /**
     * Define the max number of records to read from Redis PubSub.
     * When this max number of records is lower than {@code Long.MAX_VALUE}, the {@link Read}
     * will provide a bounded {@link PCollection}.
     *
     * @param maxNumRecords The max number of records to read from Redis PubSub.
     * @return The corresponding Read {@link PTransform}.
     */
    public Read withMaxNumRecords(long maxNumRecords) {
      checkArgument(maxReadTime() == null, "maxNumRecords and maxReadTime are exclusive");
      return builder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time on the Redis PubSub. When this max read time is defined, the
     * {@link Read} will provide a bounded {@link PCollection}.
     *
     * @param maxReadTime The max read time on the Redis PubSub.
     * @return The corresponding Read {@link PTransform}.
     */
    public Read withMaxReadTime(Duration maxReadTime) {
      checkArgument(maxNumRecords() == Long.MAX_VALUE, "maxNumRecords and maxReadTime are "
          + "exclusive");
      return builder().setMaxReadTime(maxReadTime).build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      org.apache.beam.sdk.io.Read.Unbounded<String> unbounded =
          org.apache.beam.sdk.io.Read.from(new UnboundedRedisPubSubSource(this));

      PTransform<PBegin, PCollection<String>> transform = unbounded;

      if (maxNumRecords() != Long.MAX_VALUE) {
        transform = unbounded.withMaxNumRecords(maxNumRecords());
      } else if (maxReadTime() != null) {
        transform = unbounded.withMaxReadTime(maxReadTime());
      }

      return input.getPipeline().apply(transform);
    }

    @Override
    public void validate(PBegin input) {
      // nothing to do
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      connection().populateDisplayData(builder);
      if (maxNumRecords() != Long.MAX_VALUE) {
        builder.add(DisplayData.item("maxNumRecords", maxNumRecords()));
      }
      builder.addIfNotNull(DisplayData.item("maxReadTime", maxReadTime()));
    }

  }

  private static class UnboundedRedisPubSubSource
      extends UnboundedSource<String, UnboundedSource.CheckpointMark> {

    private final Read spec;

    public UnboundedRedisPubSubSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public UnboundedReader<String> createReader(PipelineOptions pipelineOptions,
                                                CheckpointMark checkpointMark) {
      return new UnboundedRedisPubSubReader(this);
    }

    @Override
    public List<UnboundedRedisPubSubSource> generateInitialSplits(int desiredNumSplits,
                                                                  PipelineOptions pipelineOptions) {
      // Redis PubSub doesn't provide any dedup, so we create an unique subscriber
      return Collections.singletonList(new UnboundedRedisPubSubSource(spec));
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
    }

    @Override
    public Coder getCheckpointMarkCoder() {
      return VoidCoder.of();
    }

    @Override
    public Coder<String> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }

  }

  private static class UnboundedRedisPubSubReader extends UnboundedSource.UnboundedReader<String> {

    private final UnboundedRedisPubSubSource source;

    private Jedis jedis;
    private JedisPubSub jedisPubSubCallback;
    private final BlockingQueue<String> queue;
    private String current;
    private Instant currentTimestamp;

    public UnboundedRedisPubSubReader(UnboundedRedisPubSubSource source) {
      this.source = source;
      this.queue = new LinkedBlockingQueue<>();
    }

    @Override
    public boolean start() throws IOException {
      LOG.debug("Starting Redis PubSub reader");
      final Read spec = source.spec;
      jedis = spec.connection().connect();
      jedisPubSubCallback = new JedisPubSub() {
        @Override
        public void onMessage(String channel, String message) {
          currentTimestamp = Instant.now();
          try {
            queue.put(message);
          } catch (Exception e) {
            LOG.warn("Can't put into the blocking queue", e);
          }
        }

        @Override
        public void onPMessage(String pattern, String channel, String message) {
          currentTimestamp = Instant.now();
          try {
            queue.put(message);
          } catch (Exception e) {
            LOG.warn("Can't put into the blocking queue", e);
          }
        }
      };
      if (spec.channels() != null) {
        new Thread() {
          @Override
          public void run() {
            jedis.subscribe(jedisPubSubCallback, spec.channels().toArray
                (new String[spec.channels().size()]));
          }
        }.start();
      }
      if (spec.patterns() != null) {
        new Thread() {
          @Override
          public void run() {
            jedis.psubscribe(jedisPubSubCallback, spec.patterns()
                .toArray(new String[spec.patterns().size()]));
          }
        }.start();
      }
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      String message = null;
      try {
        message = queue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      if (message == null) {
        current = null;
        currentTimestamp = null;
        return false;
      }

      current = message;
      currentTimestamp = Instant.now();

      return true;
    }

    @Override
    public void close() throws IOException {
      LOG.debug("Closing Redis PubSub reader");
      jedisPubSubCallback.unsubscribe();
      jedisPubSubCallback.punsubscribe();
      try {
        jedis.quit();
      } catch (Exception e) {
        // nothing to do
      }
    }

    @Override
    public Instant getWatermark() {
      return currentTimestamp;
    }

    @Override
    public String getCurrent() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public UnboundedRedisPubSubSource getCurrentSource() {
      return source;
    }

    @Override
    public Instant getCurrentTimestamp() {
      return currentTimestamp;
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
      return new UnboundedSource.CheckpointMark() {
        @Override
        public void finalizeCheckpoint() throws IOException {
          // nothing to do
        }
      };
    }

  }

  /**
   * A {@link PTransform} to publish messages to Redis PubSub channels.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, PDone> {

    @Nullable abstract RedisConnection connection();
    @Nullable abstract List<String> channels();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnection(RedisConnection connection);
      abstract Builder setChannels(List<String> channels);
      abstract Write build();
    }

    /**
     * Define the connection to the Redis PubSub broker.
     *
     * @param connection The {@link RedisConnection}.
     * @return The corresponding {@link Write} {@link PTransform}.
     */
    public Write withConnection(RedisConnection connection) {
      checkArgument(connection != null, "RedisPubSubIO.write().withConnection"
          + "(connection) called with null connection");
      return builder().setConnection(connection).build();
    }

    public Write withChannels(List<String> channels) {
      checkArgument(channels != null, "RedisPubSubIO.write().withChannels(channels) called with"
          + " null channels");
      checkArgument(!channels.isEmpty(), "RedisPubSubIO.write().withChannels(channels) called "
          + "with empty channels list");
      return builder().setChannels(channels).build();
    }

    @Override
    public PDone expand(PCollection<String> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<String> input) {
      // validate is performed in the connection create()
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      connection().populateDisplayData(builder);
    }

    private static class WriteFn extends DoFn<String, Void> {

      private final Write spec;
      private transient Jedis jedis;

      public WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void createJedis() throws Exception {
        LOG.debug("Creating Jedis");
        jedis = spec.connection().connect();
      }

      @ProcessElement
      public void processElement(ProcessContext processContext) throws Exception {
        String element = processContext.element();
        for (String channel : spec.channels()) {
          jedis.publish(channel, element);
        }
      }

      @Teardown
      public void quitJedis() throws Exception {
        LOG.debug("Quit Jedis");
        jedis.quit();
      }

    }

  }

}
