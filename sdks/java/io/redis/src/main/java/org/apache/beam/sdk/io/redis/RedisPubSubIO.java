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
import java.io.Serializable;
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
 * An unbounded source for Redis PubSub.
 *
 * <h3>Reading from a Redis channel</h3>
 *
 * <p>RedisPubSubIO source returns an unbounded {@link PCollection} containing messages (as
 * {@code String}).
 *
 * <p>To configure a Redis PubSub source, you have to provide a Redis connection configuration
 * including {@code host} and {@code port} to connect to the Redis server, and your choice of
 * channels or patterns where to subscribe. The following example illustrates various options for
 * configuring the source:
 *
 * <pre>{@code
 *
 *  pipeline.apply(
 *    RedisPubSubIO.read()
 *      .withConnectionConfiguration(
 *        RedisPubSubIO.ConnectionConfiguration.create("localhost", 6379)
 *          .withChannels(Collections.singletonList("CHANNEL"))
 *
 * }</pre>
 *
 * <h3>Writing to a Redis channel</h3>
 *
 * <p>RedisPubSubIO sink supports writing {@code String} to a Redis channel.
 *
 * <p>To configure a Redis PubSub sink, as for the read, you have to specify a Redis connection
 * configuration with {@code host}, {@code port}.
 *
 * <p>For instance:
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(...) // provide PCollection<String>
 *    .apply(RedisPubSubIO.write().withConnectionConfiguration(RedisPubSubIO
 *    .ConnectionConfiguration.create("localhost", 6379).withChannels(Collections.singletonList
 *    ("CHANNEL")));
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
   * Describe a connection to the Redis server.
   */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    @Nullable abstract String host();
    abstract int port();
    @Nullable abstract List<String> channels();
    @Nullable abstract List<String> patterns();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setHost(String host);
      abstract Builder setPort(int port);
      abstract Builder setChannels(List<String> channels);
      abstract Builder setPatterns(List<String> pattern);
      abstract ConnectionConfiguration build();
    }

    /**
     * Describe a connection configuration to Redis.
     *
     * @param host The hostname where Redis is running.
     * @param port The port number where Redis is running.
     * @return The corresponding Redis connection configuration.
     */
    public static ConnectionConfiguration create(String host, int port) {
      checkArgument(host != null, "RedisPubSubIO.ConnectionConfiguration.create"
          + "(host, port) called with null host");
      checkArgument(port >= 0, "RedisPubSubIO.ConnectionConfiguration.create(host, port) called"
          + " with invalid port number");
      return new AutoValue_RedisPubSubIO_ConnectionConfiguration.Builder()
          .setHost(host).setPort(port).build();
    }

    /**
     * Define the channels where to subscribe or publish messages.
     *
     * @param channels The list of channels.
     * @return The corresponding Redis connection configuration.
     */
    public ConnectionConfiguration withChannels(List<String> channels) {
      checkArgument(channels != null, "RedisPubSubIO.ConnectionConfiguration.withChannels"
          + "(channels) called with null channels");
      checkArgument(channels.size() > 0, "RedisPubSubIO.ConnectionConfiguration.withChannels"
          + "(channels) called with empty channels list");
      return builder().setChannels(channels).build();
    }

    /**
     * Define the channel patterns where to subscribe or publish messages.
     *
     * @param patterns The channel patterns.
     * @return The corresponding Redis connection configuration.
     */
    public ConnectionConfiguration withPatterns(List<String> patterns) {
      checkArgument(patterns != null, "RedisPubSubIO.ConnectionConfiguration.withPatterns"
          + "(patterns) called with null patterns");
      checkArgument(patterns.size() > 0, "RedisPubSubIO.ConnectionConfiguration.withPatterns"
          + "(patterns) called with empty patterns list");
      return builder().setPatterns(patterns).build();
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("host", host()));
      builder.add(DisplayData.item("port", port()));
      if (channels() != null) {
        for (String channel : channels()) {
          builder.add(DisplayData.item("channel", channel));
        }
      }
      if (patterns() != null) {
        for (String pattern : patterns()) {
          builder.add(DisplayData.item("pattern", pattern));
        }
      }
    }

    /**
     * Create connection to Redis using Jedis.
     *
     * @return The Jedis cluster.
     */
    protected Jedis createJedis() {
      return new Jedis(host(), port());
    }

  }

  /**
   * A {@link PTransform} to read from Redis PubSub.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    @Nullable abstract ConnectionConfiguration connectionConfiguration();
    abstract long maxNumRecords();
    @Nullable abstract Duration maxReadTime();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration configuration);
      abstract Builder setMaxNumRecords(long maxNumRecords);
      abstract Builder setMaxReadTime(Duration maxReadTime);
      abstract Read build();
    }

    /**
     * Define the connection configuration to access the Redis PubSub broker.
     *
     * @param configuration The connection configuration.
     * @return The corresponding Read {@link PTransform}.
     */
    public Read withConnectionConfiguration(ConnectionConfiguration configuration) {
      checkArgument(configuration != null, "RedisPubSubIO.read().withConnectionConfiguration"
          + "(configuration) called with null configuration");
      return builder().setConnectionConfiguration(configuration).build();
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
      // validation is performed in the ConnectionConfiguration create()
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      connectionConfiguration().populateDisplayData(builder);
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
    private BlockingQueue<String> queue;
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
      jedis = spec.connectionConfiguration().createJedis();
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
      if (spec.connectionConfiguration().channels() != null) {
        new Thread() {
          @Override
          public void run() {
            jedis.subscribe(jedisPubSubCallback, spec.connectionConfiguration().channels().toArray
                (new String[spec.connectionConfiguration().channels().size()]));
          }
        }.start();
      }
      if (spec.connectionConfiguration().patterns() != null) {
        new Thread() {
          @Override
          public void run() {
            jedis.psubscribe(jedisPubSubCallback, spec.connectionConfiguration().patterns()
                .toArray(new String[spec.connectionConfiguration().patterns().size()]));
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

    @Nullable abstract ConnectionConfiguration connectionConfiguration();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration configuration);
      abstract Write build();
    }

    /**
     * Define the connection configuration to access the Redis PubSub broker.
     *
     * @param configuration The connection configuration.
     * @return The corresponding Write {@link PTransform}.
     */
    public Write withConnectionConfiguration(ConnectionConfiguration configuration) {
      checkArgument(configuration != null, "RedisPubSubIO.write().withConnectionConfiguration"
          + "(configuration) called with null configuration");
      return builder().setConnectionConfiguration(configuration).build();
    }

    @Override
    public PDone expand(PCollection<String> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<String> input) {
      // validate is performed in the ConnectionConfiguration create()
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      connectionConfiguration().populateDisplayData(builder);
    }

    private static class WriteFn extends DoFn<String, Void> {

      private final Write spec;
      private transient Jedis jedis;

      public WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void createJedis() throws Exception {
        jedis = spec.connectionConfiguration().createJedis();
      }

      @ProcessElement
      public void processElement(ProcessContext processContext) throws Exception {
        String element = processContext.element();
        for (String channel : spec.connectionConfiguration().channels()) {
          jedis.publish(channel, element);
        }
      }

      @Teardown
      public void quitJedis() throws Exception {
        jedis.quit();
      }

    }

  }

}
