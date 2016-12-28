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
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

/**
 * An IO to manipulate Redis key/value database.
 *
 * <h3>Reading Redis key/value pairs</h3>
 *
 * <p>RedisIO.Read provides a source which returns a bounded {@link PCollection} containing
 * key/value pairs as {@code KV<String, String>}.
 *
 * <p>To configure a Redis source, you have to provide Redis server hostname and port number.
 * Optionally, you can provide a key pattern (to filter the keys). The following example
 * illustrates how to configure a source:
 *
 * <pre>{@code
 *
 *  pipeline.apply(RedisIO.read()
 *    .withConnection(RedisConnection.create(Collections.singletonList("localhost:6379")))
 *    .withKeyPattern("foo*")
 *
 * }</pre>
 *
 * <h3>Writing Redis key/value pairs</h3>
 *
 * <p>RedisIO.Write provides a sink to write key/value pairs into a Redis database.
 *
 * <p>To configure a Redis sink, as for the source, you have to provide Redis server hostname and
 * port number. Optionally, you can provide the Redis command to set/update the key/value pairs.
 * The following example illustrates how to configure a sink:
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(...) // provide PCollection<KV<String, String>>
 *    .apply(RedisIO.write()
 *      .withConnection(RedisConnection.create(Collections.singletonList("localhost:6379")))
 *
 * }</pre>
 *
 */
public class RedisIO {

  private static final Logger LOG = LoggerFactory.getLogger(RedisIO.class);

  public static Read read() {
    return new  AutoValue_RedisIO_Read.Builder()
        .setCommand(Read.Command.GET).setKeyPattern("*").build();
  }

  public static Write write() {
    return new AutoValue_RedisIO_Write.Builder()
        .setCommand(Write.Command.SET).build();
  }

  private RedisIO() {
  }

  /**
   * A {@link PTransform} reading key/value pairs from a Redis database.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<KV<String, String>>> {

    /**
     * The Redis commands related to read of key-value pairs.
     */
    public enum Command {
      GET
    }

    @Nullable abstract RedisConnection connection();
    @Nullable abstract Command command();
    @Nullable abstract String keyPattern();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      @Nullable abstract Builder setConnection(RedisConnection connection);
      @Nullable abstract Builder setCommand(Command command);
      @Nullable abstract Builder setKeyPattern(String keyPattern);
      abstract Read build();
    }

    /**
     * Define the connection to the Redis server.
     *
     * @param connection The {@link RedisConnection}.
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withConnection(RedisConnection connection) {
      checkArgument(connection != null, "RedisIO.read().withConnection(connection) called with "
          + "null connection");
      return builder().setConnection(connection).build();
    }

    /**
     * Define the Redis command to execute to retrieve the key/value pairs.
     *
     * @param command The Redis command.
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withCommand(Command command) {
      checkArgument(command != null, "RedisIO.read().withCommand(command) called with null "
          + "command");
      return builder().setCommand(command).build();
    }

    /**
     * Define the pattern to filter the Redis keys.
     * @param keyPattern The filter key pattern.
     * @return The corresponding {@link Read} {@link PTransform}.
     */
    public Read withKeyPattern(String keyPattern) {
      checkArgument(keyPattern != null, "RedisIO.read().withKeyPattern(pattern) called with "
          + "null pattern");
      return builder().setKeyPattern(keyPattern).build();
    }

    @Override
    public void validate(PBegin input) {
      checkState(connection() != null, "RedisIO.read() requires a connection to be set "
          + "withConnection(connection)");
      checkState(command() != null,  "RedisIO.read() requires a command to be set via "
          + "withCommand(command)");
      checkState(keyPattern() != null, "RedisIO.read() requires a key pattern to be set via "
          + "withKeyPattern(keyPattern");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      connection().populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("command", command().toString()));
    }

    @Override
    public PCollection<KV<String, String>> expand(PBegin input) {
      return input.getPipeline()
          .apply(org.apache.beam.sdk.io.Read.from(new BoundedRedisSource(this)));
    }

  }

  /**
   * A bounded source reading key-value pairs from a Redis server.
   */
  // visible for testing
  protected static class BoundedRedisSource extends BoundedSource<KV<String, String>> {

    private Read spec;

    protected BoundedRedisSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public List<BoundedRedisSource> splitIntoBundles(long desiredBundleSizeBytes,
                                                     PipelineOptions pipelineOptions) {
      // TODO cluster with one source per slot
      return Collections.singletonList(this);
    }

    /**
     * The estimate size bytes is based on sampling, computing average size of 10 random
     * key/value pairs. This sampling average size is used with the Redis dbSize to get an
     * estimation of the actual database size.
     *
     * @param pipelineOptions The pipeline options.
     * @return The estimated size of the Redis database in bytes.
     */
    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws Exception {
      Jedis jedis = spec.connection().connect();
      // estimate the size of a key/value pair using sampling
      long samplingSize = 0;
      for (int i = 0; i < 10; i++) {
        String key = jedis.randomKey();
        if (key != null) {
          samplingSize = samplingSize + key.getBytes().length;
          String value = jedis.get(key);
          if (value != null) {
            samplingSize = samplingSize + value.getBytes().length;
          }
        }
      }
      long samplingAverage = samplingSize / 10;
      // db size
      long dbSize = jedis.dbSize();
      jedis.quit();
      return dbSize * samplingAverage;
    }

    @Override
    public BoundedReader<KV<String, String>> createReader(PipelineOptions pipelineOptions) {
      return new RedisBoundedReader(this);
    }

    @Override
    public void validate() {
      // done in the Read
    }

    @Override
    public Coder<KV<String, String>> getDefaultOutputCoder() {
      return KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
    }

  }

  private static class RedisBoundedReader extends BoundedSource.BoundedReader<KV<String, String>> {

    private final BoundedRedisSource source;

    private Jedis jedis;
    private Iterator<String> keysIterator;
    private KV<String, String> current;

    public RedisBoundedReader(BoundedRedisSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      LOG.debug("Starting Redis reader");
      Read spec = source.spec;
      try {
        jedis = spec.connection().connect();
        Set<String> keys = jedis.keys(spec.keyPattern());
        keysIterator = keys.iterator();
        return advance();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public boolean advance() {
      if (keysIterator.hasNext()) {
        String key = keysIterator.next();
        String value = jedis.get(key);
        KV<String, String> kv = KV.of(key, value);
        current = kv;
        return true;
      }
      return false;
    }

    @Override
    public void close() {
      LOG.debug("Closing Redis reader");
      jedis.quit();
    }

    @Override
    public KV<String, String> getCurrent() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public BoundedSource<KV<String, String>> getCurrentSource() {
      return source;
    }

  }

  /**
   * A {@link PTransform} to store or update key/value pair in a Redis database.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<KV<String, String>>, PDone> {

    /**
     * Redis commands related to store of key-value pairs.
     */
    public enum Command {
      SET, APPEND, SETNX
    }

    @Nullable abstract RedisConnection connection();
    @Nullable abstract Command command();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnection(RedisConnection connection);
      abstract Builder setCommand(Command command);
      abstract Write build();
    }

    /**
     * Define the connection to the Redis server.
     *
     * @param connection The Redis server connection configuration.
     * @return The corresponding {@link Write} {@link PTransform}.
     */
    public Write withConnection(RedisConnection connection) {
      checkArgument(connection != null, "RedisIO.write().withConnection(connection) called with"
          + " null connection");
      return builder().setConnection(connection).build();
    }

    public Write withCommand(Command command) {
      return builder().setCommand(command).build();
    }

    @Override
    public PDone expand(PCollection<KV<String, String>> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<KV<String, String>> input) {
      checkState(command() != null, "Command is null");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      connection().populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("command", command().toString()));
    }

    private static class WriteFn extends DoFn<KV<String, String>, Void> {

      private Write spec;
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
      public void processElements(ProcessContext processContext) throws Exception {
        KV<String, String> element = processContext.element();
        if (spec.command() == Command.SET) {
          jedis.set(element.getKey(), element.getValue());
        }
        if (spec.command() == Command.APPEND) {
          jedis.append(element.getKey(), element.getValue());
        }
        if (spec.command() == Command.SETNX) {
          jedis.setnx(element.getKey(), element.getValue());
        }
        /*
        jedis.incrBy(element.getKey(), Long.parseLong(element.getValue()));
        jedis.decrBy(element.getKey(), Long.parseLong(element.getValue()));
        jedis.del(element.getKey());
        jedis.rename(element.getKey(), element.getValue());
        jedis.renamenx(element.getKey(), element.getValue());
        jedis.expire(element.getKey(), Integer.parseInt(element.getValue()));
        jedis.expireAt(element.getKey(), Long.parseLong(element.getValue()));
        jedis.pexpire(element.getKey(), Integer.parseInt(element.getValue()));
        jedis.pexpireAt(element.getKey(), Long.parseLong(element.getValue()));
        jedis.move(element.getKey(), Integer.parseInt(element.getValue()));
        jedis.incrByFloat(element.getKey(), Float.parseFloat(element.getValue()));
        jedis.hdel(element.getKey(), element.getValue());
        jedis.rpush(element.getKey(), element.getValue());
        jedis.lpush(element.getKey(), element.getValue());
        jedis.rpoplpush(element.getKey(), element.getValue());
        jedis.sadd(element.getKey(), element.getValue());
        jedis.srem(element.getKey(), element.getValue());
        */
      }

      @Teardown
      public void quitJedis() throws Exception {
        LOG.debug("Quit Jedis");
        jedis.quit();
      }

    }

  }

}
