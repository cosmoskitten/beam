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
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.clickhouse.TableSchema.ColumnType;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;

/** An IO to write to ClickHouse. */
public class ClickHouseIO {

  /** This {@link DoFn} reads table schemas from ClickHouse. */
  @AutoValue
  public abstract static class ReadTableSchemaFn extends DoFn<Void, TableSchema> {
    private ClickHouseConnection connection;

    public abstract String jdbcUrl();

    public abstract String table();

    public static ReadTableSchemaFn of(String jdbcUrl, String table) {
      return new AutoValue_ClickHouseIO_ReadTableSchemaFn(jdbcUrl, table);
    }

    @Setup
    public void setup() throws SQLException {
      connection = new ClickHouseDataSource(jdbcUrl()).getConnection();
    }

    @Teardown
    public void tearDown() throws Exception {
      connection.close();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws SQLException {
      ResultSet rs = null;
      try (Statement statement = connection.createStatement()) {
        rs = statement.executeQuery("DESCRIBE TABLE " + table());
        List<TableSchema.Column> columns = new ArrayList<>();

        while (rs.next()) {
          String name = rs.getString("name");
          String type = rs.getString("type");

          ColumnType columnType = ColumnType.parse(type);

          columns.add(TableSchema.Column.of(name, columnType));
        }

        c.output(TableSchema.of(columns));
      } finally {
        // findbugs doesn't like double resources
        if (rs != null) {
          rs.close();
        }
      }
    }
  }

  /** A {@link PTransform} to write to ClickHouse. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<Row>, PDone> {
    public abstract String jdbcUrl();

    public abstract String table();

    @Override
    public PDone expand(PCollection<Row> input) {
      PCollection<Void> schemaSeed =
          input.getPipeline().apply("Create Seed", Create.of((Void) null));

      PCollectionView<TableSchema> schemaView =
          schemaSeed
              .apply("Read Table Schema", ParDo.of(ReadTableSchemaFn.of(jdbcUrl(), table())))
              .apply("Table Schema View", View.asSingleton());

      input.apply(
          ParDo.of(WriteFn.create(jdbcUrl(), table(), schemaView)).withSideInputs(schemaView));

      return PDone.in(input.getPipeline());
    }

    public static Builder builder() {
      return new AutoValue_ClickHouseIO_Write.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {

      public abstract Builder jdbcUrl(String jdbcUrl);

      public abstract Builder table(String table);

      public abstract Write build();
    }
  }

  @AutoValue
  abstract static class WriteFn extends DoFn<Row, Void> {

    private ClickHouseConnection connection;

    public abstract String jdbcUrl();

    public abstract String table();

    public abstract PCollectionView<TableSchema> schema();

    public static WriteFn create(
        String jdbcUrl, String table, PCollectionView<TableSchema> schema) {
      return new AutoValue_ClickHouseIO_WriteFn(jdbcUrl, table, schema);
    }

    public static void writeRow(ClickHouseRowBinaryStream stream, TableSchema schema, Row row)
        throws IOException {
      for (int i = 0; i < schema.columns().size(); i++) {
        long value = row.getInt64(i);
        stream.writeInt64(value);
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
              .map(x -> quoteIdentifier(x.name()))
              .collect(Collectors.joining(", "));
      return "INSERT INTO " + quoteIdentifier(table) + " (" + columnsStr + ")";
    }

    public void write(TableSchema schema, Iterator<Row> rows) throws SQLException {

      try (ClickHouseStatement statement = connection.createStatement()) {
        statement.sendRowBinaryStream(
            insertSql(schema, table()),
            stream -> {
              while (rows.hasNext()) {
                Row row = rows.next();
                writeRow(stream, schema, row);
              }
            });
      }
    }

    @Setup
    public void setup() throws SQLException {
      connection = new ClickHouseDataSource(jdbcUrl()).getConnection();
    }

    @Teardown
    public void tearDown() throws Exception {
      connection.close();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws SQLException {
      Row row = c.element();
      TableSchema schema = c.sideInput(schema());
      write(schema, Collections.singletonList(row).iterator());
    }
  }
}
