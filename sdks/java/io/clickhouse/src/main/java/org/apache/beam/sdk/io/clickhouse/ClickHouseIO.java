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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.clickhouse.TableSchema.ColumnType;
import org.apache.beam.sdk.io.clickhouse.TableSchema.DefaultType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Days;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;

/** An IO to write to ClickHouse. */
public class ClickHouseIO {

  /** A {@link PTransform} to write to ClickHouse. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<Row>, PDone> {
    public abstract String jdbcUrl();

    public abstract String table();

    @Nullable
    public abstract Properties properties();

    public static TableSchema getTableSchema(String jdbcUrl, String table) {
      ResultSet rs;
      try (ClickHouseConnection connection = new ClickHouseDataSource(jdbcUrl).getConnection();
          Statement statement = connection.createStatement()) {
        rs = statement.executeQuery("DESCRIBE TABLE " + table);
        List<TableSchema.Column> columns = new ArrayList<>();

        while (rs.next()) {
          String name = rs.getString("name");
          String type = rs.getString("type");
          String defaultTypeStr = rs.getString("default_type");
          String defaultExpression = rs.getString("default_expression");

          ColumnType columnType = ColumnType.parse(type);
          DefaultType defaultType = DefaultType.parse(defaultTypeStr).orElse(null);

          Object defaultValue;
          if (DefaultType.DEFAULT.equals(defaultType)
              && !Strings.isNullOrEmpty(defaultExpression)) {
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

    @Override
    public PDone expand(PCollection<Row> input) {
      TableSchema tableSchema = getTableSchema(jdbcUrl(), table());
      Properties properties = properties() == null ? new Properties() : properties();

      input.apply(ParDo.of(WriteFn.create(jdbcUrl(), table(), tableSchema, properties)));

      return PDone.in(input.getPipeline());
    }

    public static Builder builder() {
      return new AutoValue_ClickHouseIO_Write.Builder();
    }

    /** Builder for {@link Write}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder jdbcUrl(String jdbcUrl);

      public abstract Builder table(String table);

      public abstract Builder properties(Properties properties);

      public abstract Write build();
    }
  }

  public static ClickHouseProperties properties() {
    return new ClickHouseProperties();
  }

  /**
   * Builder for {@link Properties} for JDBC connection.
   *
   * @see <a href="https://clickhouse.yandex/docs/en/single/#settings_1">ClickHouse
   *     documentation</a>
   */
  public static class ClickHouseProperties implements Serializable {
    private final Properties properties = new Properties();

    /** Maximum block size for reading. */
    public ClickHouseProperties maxBlockSize(int value) {
      return set(ClickHouseQueryParam.MAX_BLOCK_SIZE, value);
    }

    /** The maximum block size for insertion, if we control the creation of blocks for insertion. */
    public ClickHouseProperties maxInsertBlockSize(long value) {
      return set(ClickHouseQueryParam.MAX_INSERT_BLOCK_SIZE, value);
    }

    /** If setting is enabled, insert query into distributed waits until data will be sent to all nodes in cluster. */
    public ClickHouseProperties insertDistributedSync(boolean value) {
      return set("insert_distributed_sync", value ? 1 : 0);
    }

    /** For INSERT queries in the replicated table, wait writing for the specified number of replicas and linearize the addition of the data. 0 - disabled. */
    public ClickHouseProperties insertQuorum(long value) {
      return set(ClickHouseQueryParam.INSERT_QUORUM, value);
    }

    /** For INSERT queries in the replicated table, specifies that deduplication of inserting
     * blocks should be preformed. */
    public ClickHouseProperties insertDeduplicate(boolean value) {
      return set("insert_deduplicate", value ? 1L : 0L);
    }

    public ClickHouseProperties set(ClickHouseQueryParam param, Object value) {
      Preconditions.checkArgument(param.getClazz().isInstance(value));
      properties.put(param, value);
      return this;
    }

    public ClickHouseProperties set(String param, Object value) {
      properties.put(param, value);
      return this;
    }

    public Properties build() {
      return properties;
    }
  }

  @AutoValue
  abstract static class WriteFn extends DoFn<Row, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(WriteFn.class);
    private static final int QUEUE_SIZE = 1024;

    private ClickHouseConnection connection;
    private ClickHouseStatement statement;

    private ExecutorService executor;
    private BlockingQueue<Row> queue;
    private AtomicBoolean bundleFinished;
    private Future<?> insert;

    public abstract String jdbcUrl();

    public abstract String table();

    public abstract TableSchema schema();

    public abstract Properties properties();

    public static WriteFn create(
        String jdbcUrl, String table, TableSchema schema, Properties properties) {
      return new AutoValue_ClickHouseIO_WriteFn(jdbcUrl, table, schema, properties);
    }

    static String quoteIdentifier(String identifier) {
      String backslash = "\\\\";
      String quote = "\"";

      return quote + identifier.replaceAll(quote, backslash + quote) + quote;
    }

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
      connection = new ClickHouseDataSource(jdbcUrl(), properties()).getConnection();
      executor = Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder().setNameFormat("clickhouse-jdbc-%d").build());
      queue = new ArrayBlockingQueue<>(QUEUE_SIZE);
      bundleFinished = new AtomicBoolean(false);
    }

    @StartBundle
    public void startBundle() throws SQLException {
      statement = connection.createStatement();
      bundleFinished.set(false);

      // When bundle starts, we open statement and stream data in bundle.
      // When we finish bundle, we close statement.

      // For streaming, we create a background thread for http client,
      // and we feed it through BlockingQueue.

      insert =
          executor.submit(
              () -> {
                try {
                  statement.sendRowBinaryStream(
                      insertSql(schema(), table()),
                      stream -> {
                        while (true) {
                          Row row;
                          try {
                            row = queue.poll(1, TimeUnit.SECONDS);
                          } catch (InterruptedException e) {
                            break; // time to go
                          }

                          if (row == null) {
                            if (bundleFinished.get() && queue.isEmpty()) {
                              // bundle is finished and queue is drained, time to go
                              break;
                            } else {
                              // timeout, let's try again
                            }
                          } else {
                            ClickHouseWriter.writeRow(stream, schema(), row);
                          }
                        }
                      });
                  statement.close();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                } finally {
                  try {
                    if (!statement.isClosed()) {
                      // happens only if statement wasn't closed in the end of the previous block
                      statement.close();
                    }
                  } catch (SQLException e) {
                    LOG.error("Failed to close statement", e);
                  }
                }
              });
    }

    @FinishBundle
    public void finishBundle() throws ExecutionException, InterruptedException {
      bundleFinished.set(true);
      insert.get(); // drain queue
    }

    @Teardown
    public void tearDown() throws Exception {
      bundleFinished.set(true);
      executor.shutdown();
      connection.close();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws InterruptedException, ExecutionException {
      while (!queue.offer(c.element(), 1, TimeUnit.SECONDS)) {
        if (insert.isDone()) {
          insert.get(); // can happen due to exception, get() will throw it
          throw new AssertionError("Invariant failed");
        }
      }
    }
  }
}
