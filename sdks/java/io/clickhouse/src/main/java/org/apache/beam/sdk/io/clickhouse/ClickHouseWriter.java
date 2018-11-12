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

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.clickhouse.TableSchema.ColumnType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Days;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;

/** Writes Rows and field values using {@link ClickHouseRowBinaryStream}. */
@Experimental(Experimental.Kind.SCHEMAS)
public class ClickHouseWriter {
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

  @SuppressWarnings("unchecked")
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
          if (value == null) {
            value = column.defaultValue();
          }

          writeValue(stream, column.columnType(), value);
        }
      }
    }
  }
}
