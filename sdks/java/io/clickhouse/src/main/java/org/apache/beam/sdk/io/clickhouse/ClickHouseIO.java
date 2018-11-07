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

        return TableSchema.of(columns);
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

    @AutoValue.Builder
    abstract static class Builder {

      public abstract Builder jdbcUrl(String jdbcUrl);

      public abstract Builder table(String table);

      public abstract Builder properties(final Properties properties);

      public abstract Write build();
    }
  }

  public static ClickHouseProperties properties() {
    return new ClickHouseProperties();
  }

  static class ClickHouseProperties implements Serializable {
    private final Properties properties = new Properties();

    public ClickHouseProperties maxBlockSize(int value) {
      return set(ClickHouseQueryParam.MAX_BLOCK_SIZE, value);
    }

    public ClickHouseProperties maxInsertBlockSize(long value) {
      return set(ClickHouseQueryParam.MAX_INSERT_BLOCK_SIZE, value);
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

    private static final Instant EPOCH_INSTANT = new Instant(0L);

    @SuppressWarnings("unchecked")
    public static void writeNullableValue(
        ClickHouseRowBinaryStream stream, ColumnType columnType, Object value) throws IOException {

      if (value == null) {
        stream.writeByte((byte) 1);
      } else {
        stream.writeByte((byte) 0);
        writeValue(stream, columnType, value);
      }
    }

    public static void writeValue(
        ClickHouseRowBinaryStream stream, ColumnType columnType, Object value) throws IOException {

      switch (columnType.typeName()) {
        case FLOAT32:
          stream.writeFloat32((Float) value);
          break;

        case FLOAT64:
          stream.writeFloat64((Double) value);
          break;

        case INT8:
          stream.writeInt8((Byte) value);
          break;

        case INT16:
          stream.writeInt16((Short) value);
          break;

        case INT32:
          stream.writeInt32((Integer) value);
          break;

        case INT64:
          stream.writeInt64((Long) value);
          break;

        case STRING:
          stream.writeString((String) value);
          break;

        case UINT8:
          stream.writeUInt8((Short) value);
          break;

        case UINT16:
          stream.writeUInt16((Integer) value);
          break;

        case UINT32:
          stream.writeUInt32((Long) value);
          break;

        case UINT64:
          stream.writeUInt64((Long) value);
          break;

        case DATE:
          Days epochDays = Days.daysBetween(EPOCH_INSTANT, (ReadableInstant) value);
          stream.writeUInt16(epochDays.getDays());
          break;

        case DATETIME:
          long epochSeconds = ((ReadableInstant) value).getMillis() / 1000L;
          stream.writeUInt32(epochSeconds);
          break;

        case ARRAY:
          List<Object> values = (List<Object>) value;
          stream.writeUnsignedLeb128(values.size());
          for (Object arrayValue : values) {
            writeValue(stream, columnType.arrayElementType(), arrayValue);
          }
          break;
      }
    }

    public static void writeRow(ClickHouseRowBinaryStream stream, TableSchema schema, Row row)
        throws IOException {
      for (TableSchema.Column column : schema.columns()) {
        if (!column.materializedOrAlias()) {
          Object value = row.getValue(column.name());

          if (column.columnType().nullable()) {
            writeNullableValue(stream, column.columnType(), value);
          } else {
            writeValue(stream, column.columnType(), value);
          }
        }
      }
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
      executor = Executors.newSingleThreadExecutor();
      queue = new ArrayBlockingQueue<>(1024);
      bundleFinished = new AtomicBoolean(false);
    }

    @StartBundle
    public void startBundle(StartBundleContext context) throws SQLException {
      statement = connection.createStatement();
      bundleFinished.set(false);

      insert =
          executor.submit(
              () -> {
                try {
                  statement.sendRowBinaryStream(
                      insertSql(schema(), table()),
                      stream -> {
                        while (true) {
                          Row row = null;
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
                            writeRow(stream, schema(), row);
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
          insert.get(); // can only happen due to exception, let's throw it
        }
      }
    }
  }
}
