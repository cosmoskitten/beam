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
package org.apache.beam.sdk.io.clickhouse;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.clickhouse.TableSchema.ColumnType;
import org.apache.beam.sdk.io.clickhouse.TableSchema.DefaultType;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

/** An IO to write to ClickHouse. */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class ClickHouseIO {

  public static final long DEFAULT_MAX_INSERT_BLOCK_SIZE = 1000000;
  public static final int DEFAULT_MAX_RETRIES = 5;
  public static final Duration DEFAULT_MAX_CUMULATIVE_BACKOFF = Duration.standardDays(1000);
  public static final Duration DEFAULT_INITIAL_BACKOFF = Duration.standardSeconds(5);

  public static <T> Write<T> write(String jdbcUrl, String table) {
    return new AutoValue_ClickHouseIO_Write.Builder<T>()
        .jdbcUrl(jdbcUrl)
        .table(table)
        .properties(new Properties())
        .maxInsertBlockSize(DEFAULT_MAX_INSERT_BLOCK_SIZE)
        .initialBackoff(DEFAULT_INITIAL_BACKOFF)
        .maxRetries(DEFAULT_MAX_RETRIES)
        .maxCumulativeBackoff(DEFAULT_MAX_CUMULATIVE_BACKOFF)
        .build()
        .withInsertDeduplicate(true)
        .withInsertDistributedSync(true);
  }

  /** A {@link PTransform} to write to ClickHouse. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

    public abstract String jdbcUrl();

    public abstract String table();

    public abstract Properties properties();

    public abstract long maxInsertBlockSize();

    public abstract int maxRetries();

    public abstract Duration maxCumulativeBackoff();

    public abstract Duration initialBackoff();

    @Nullable
    public abstract Boolean insertDistributedSync();

    @Nullable
    public abstract Long insertQuorum();

    @Nullable
    public abstract Boolean insertDeduplicate();

    abstract Builder<T> toBuilder();

    @Override
    public PDone expand(PCollection<T> input) {
      TableSchema tableSchema = getTableSchema(jdbcUrl(), table());
      Properties properties = properties();

      set(properties, ClickHouseQueryParam.MAX_INSERT_BLOCK_SIZE, maxInsertBlockSize());
      set(properties, ClickHouseQueryParam.INSERT_QUORUM, insertQuorum());
      set(properties, "insert_distributed_sync", insertDistributedSync());
      set(properties, "insert_deduplication", insertDeduplicate());

      WriteFn<T> fn =
          new AutoValue_ClickHouseIO_WriteFn.Builder<T>()
              .jdbcUrl(jdbcUrl())
              .table(table())
              .maxInsertBlockSize(maxInsertBlockSize())
              .schema(tableSchema)
              .properties(properties)
              .initialBackoff(initialBackoff())
              .maxCumulativeBackoff(maxCumulativeBackoff())
              .maxRetries(maxRetries())
              .build();

      input.apply(ParDo.of(fn));

      return PDone.in(input.getPipeline());
    }

    /**
     * The maximum block size for insertion, if we control the creation of blocks for insertion.
     *
     * @param value number of rows
     * @return a {@link PTransform} writing data to ClickHouse
     * @see <a href="https://clickhouse.yandex/docs/en/single/#max_insert_block_size">ClickHouse
     *     documentation</a>
     */
    public Write<T> withMaxInsertBlockSize(long value) {
      return toBuilder().maxInsertBlockSize(value).build();
    }

    /**
     * If setting is enabled, insert query into distributed waits until data will be sent to all
     * nodes in cluster.
     *
     * @param value true to enable, null for server default
     * @return a {@link PTransform} writing data to ClickHouse
     */
    public Write<T> withInsertDistributedSync(@Nullable Boolean value) {
      return toBuilder().insertDistributedSync(value).build();
    }

    /**
     * For INSERT queries in the replicated table, wait writing for the specified number of replicas
     * and linearize the addition of the data. 0 - disabled.
     *
     * @param value number of replicas, 0 for disabling, null for server default
     * @return a {@link PTransform} writing data to ClickHouse
     * @see <a href="https://clickhouse.yandex/docs/en/single/#insert_quorum">ClickHouse
     *     documentation</a>
     */
    public Write<T> withInsertQuorum(@Nullable Long value) {
      return toBuilder().insertQuorum(value).build();
    }

    /**
     * For INSERT queries in the replicated table, specifies that deduplication of inserting blocks
     * should be performed.
     *
     * <p>Enabled by default. Shouldn't be disabled unless your input has duplicate blocks, and you
     * don't want to deduplicate them.
     *
     * @param value true to enable, null for server default
     * @return a {@link PTransform} writing data to ClickHouse
     */
    public Write<T> withInsertDeduplicate(Boolean value) {
      return toBuilder().insertDeduplicate(value).build();
    }

    /**
     * Maximum number of retries per insert.
     *
     * <p>See {@link FluentBackoff#withMaxRetries}.
     *
     * @param value maximum number of retries
     * @return a {@link PTransform} writing data to ClickHouse
     */
    public Write<T> withMaxRetries(int value) {
      return toBuilder().maxRetries(value).build();
    }

    /**
     * Limits total time spent in backoff.
     *
     * <p>See {@link FluentBackoff#withMaxCumulativeBackoff}.
     *
     * @param value maximum duration
     * @return a {@link PTransform} writing data to ClickHouse
     */
    public Write<T> withMaxCumulativeBackoff(Duration value) {
      return toBuilder().maxCumulativeBackoff(value).build();
    }

    /**
     * Set initial backoff duration.
     *
     * <p>See {@link FluentBackoff#withInitialBackoff}.
     *
     * @param value initial duration
     * @return a {@link PTransform} writing data to ClickHouse
     */
    public Write<T> withInitialBackoff(Duration value) {
      return toBuilder().initialBackoff(value).build();
    }

    /** Builder for {@link Write}. */
    @AutoValue.Builder
    abstract static class Builder<T> {

      public abstract Builder<T> jdbcUrl(String jdbcUrl);

      public abstract Builder<T> table(String table);

      public abstract Builder<T> maxInsertBlockSize(long maxInsertBlockSize);

      public abstract Builder<T> insertDistributedSync(Boolean insertDistributedSync);

      public abstract Builder<T> insertQuorum(Long insertQuorum);

      public abstract Builder<T> insertDeduplicate(Boolean insertDeduplicate);

      public abstract Builder<T> properties(Properties properties);

      public abstract Builder<T> maxRetries(int maxRetries);

      public abstract Builder<T> maxCumulativeBackoff(Duration maxCumulativeBackoff);

      public abstract Builder<T> initialBackoff(Duration initialBackoff);

      public abstract Write<T> build();
    }

    private static void set(Properties properties, ClickHouseQueryParam param, Object value) {
      if (value != null) {
        Preconditions.checkArgument(
            param.getClazz().isInstance(value),
            "Unexpected value '"
                + value
                + "' for "
                + param.getKey()
                + " got "
                + value.getClass().getName()
                + ", expected "
                + param.getClazz().getName());
        properties.put(param, value);
      }
    }

    private static void set(Properties properties, String param, Object value) {
      if (value != null) {
        properties.put(param, value);
      }
    }
  }

  @AutoValue
  abstract static class WriteFn<T> extends DoFn<T, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(WriteFn.class);
    private static final String RETRY_ATTEMPT_LOG =
        "Error writing to ClickHouse. Retry attempt[%d]";

    private ClickHouseConnection connection;
    private FluentBackoff retryBackoff;
    private final List<Row> buffer = new ArrayList<>();

    // TODO: This should be the same as resolved so that Beam knows which fields
    // are being accessed. Currently Beam only supports wildcard descriptors.
    // Once BEAM-4457 is fixed, fix this.
    @FieldAccess("filterFields")
    final FieldAccessDescriptor fieldAccessDescriptor = FieldAccessDescriptor.withAllFields();

    public abstract String jdbcUrl();

    public abstract String table();

    public abstract long maxInsertBlockSize();

    public abstract int maxRetries();

    public abstract Duration maxCumulativeBackoff();

    public abstract Duration initialBackoff();

    public abstract TableSchema schema();

    public abstract Properties properties();

    @VisibleForTesting
    static String insertSql(TableSchema schema, String table) {
      String columnsStr =
          schema
              .columns()
              .stream()
              .filter(x -> !x.materializedOrAlias())
              .map(x -> quoteIdentifier(x.name()))
              .collect(Collectors.joining(", "));
      return "INSERT INTO " + quoteIdentifier(table) + " (" + columnsStr + ")";
    }

    @Setup
    public void setup() throws SQLException {
      String maxInsertBlockSizeKey = ClickHouseQueryParam.MAX_INSERT_BLOCK_SIZE.getKey();
      Properties properties = new Properties(properties());

      if (!properties.containsKey(maxInsertBlockSizeKey)) {
        properties.put(maxInsertBlockSizeKey, DEFAULT_MAX_INSERT_BLOCK_SIZE);
      }

      connection = new ClickHouseDataSource(jdbcUrl(), properties).getConnection();

      retryBackoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(maxRetries())
              .withMaxCumulativeBackoff(maxCumulativeBackoff())
              .withInitialBackoff(initialBackoff());
    }

    @Teardown
    public void tearDown() throws Exception {
      connection.close();
    }

    @StartBundle
    public void startBundle() {
      buffer.clear();
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      flush();
    }

    @ProcessElement
    public void processElement(@FieldAccess("filterFields") Row input) throws Exception {
      buffer.add(input);

      if (buffer.size() >= maxInsertBlockSize()) {
        flush();
      }
    }

    private void flush() throws Exception {
      BackOff backOff = retryBackoff.backoff();
      int attempt = 0;

      while (true) {
        try (ClickHouseStatement statement = connection.createStatement()) {
          statement.sendRowBinaryStream(
              insertSql(schema(), table()),
              stream -> {
                for (Row row : buffer) {
                  ClickHouseWriter.writeRow(stream, schema(), row);
                }
              });
          buffer.clear();
          break;
        } catch (SQLException e) {
          if (!BackOffUtils.next(Sleeper.DEFAULT, backOff)) {
            throw e;
          } else {
            LOG.warn(String.format(RETRY_ATTEMPT_LOG, attempt), e);
            attempt++;
          }
        }
      }
    }

    @AutoValue.Builder
    abstract static class Builder<T> {

      public abstract Builder<T> jdbcUrl(String jdbcUrl);

      public abstract Builder<T> table(String table);

      public abstract Builder<T> maxInsertBlockSize(long maxInsertBlockSize);

      public abstract Builder<T> schema(TableSchema schema);

      public abstract Builder<T> properties(Properties properties);

      public abstract Builder<T> maxRetries(int maxRetries);

      public abstract Builder<T> maxCumulativeBackoff(Duration maxCumulativeBackoff);

      public abstract Builder<T> initialBackoff(Duration initialBackoff);

      public abstract WriteFn<T> build();
    }
  }

  public static TableSchema getTableSchema(String jdbcUrl, String table) {
    ResultSet rs;
    try (ClickHouseConnection connection = new ClickHouseDataSource(jdbcUrl).getConnection();
        Statement statement = connection.createStatement()) {
      rs = statement.executeQuery("DESCRIBE TABLE " + quoteIdentifier(table));
      List<TableSchema.Column> columns = new ArrayList<>();

      while (rs.next()) {
        String name = rs.getString("name");
        String type = rs.getString("type");
        String defaultTypeStr = rs.getString("default_type");
        String defaultExpression = rs.getString("default_expression");

        ColumnType columnType = ColumnType.parse(type);
        DefaultType defaultType = DefaultType.parse(defaultTypeStr).orElse(null);

        Object defaultValue;
        if (DefaultType.DEFAULT.equals(defaultType) && !Strings.isNullOrEmpty(defaultExpression)) {
          defaultValue = ColumnType.parseDefaultExpression(columnType, defaultExpression);
        } else {
          defaultValue = null;
        }

        columns.add(TableSchema.Column.of(name, columnType, defaultType, defaultValue));
      }

      // findbugs doesn't like it in try block
      rs.close();

      return TableSchema.of(columns.toArray(new TableSchema.Column[0]));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  static String quoteIdentifier(String identifier) {
    String backslash = "\\\\";
    String quote = "\"";

    return quote + identifier.replaceAll(quote, backslash + quote) + quote;
  }
}
