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
package org.apache.beam.sdk.io.gcp.bigquery;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableSet;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

/** Utils to help convert Apache Avro types to Beam types. */
public class AvroUtils {
  // TODO: BigQuery shouldn't know about SQL internal logical types.
  private static final Set<String> SQL_DATE_TIME_TYPES =
      ImmutableSet.of(
          "SqlDateType", "SqlTimeType", "SqlTimeWithLocalTzType", "SqlTimestampWithLocalTzType");
  private static final Set<String> SQL_STRING_TYPES = ImmutableSet.of("SqlCharType");

  /** Tries to convert an Avro field to Beam field based on the target type of the Beam field. */
  public static Object convertAvroFormat(Field beamField, Object value) {
    TypeName beamFieldTypeName = beamField.getType().getTypeName();
    switch (beamFieldTypeName) {
      case INT16:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case BYTE:
      case BOOLEAN:
        return convertAvroPrimitiveTypes(beamFieldTypeName, value);
      case DATETIME:
        // Expecting value in microseconds.
        return safeToMillis(value);
      case STRING:
        return convertAvroPrimitiveTypes(beamFieldTypeName, value);
      case ARRAY:
        return convertAvroArray(beamField, value);
      case LOGICAL_TYPE:
        String identifier = beamField.getType().getLogicalType().getIdentifier();
        if (SQL_DATE_TIME_TYPES.contains(identifier)) {
          return safeToMillis(value);
        } else if (SQL_STRING_TYPES.contains(identifier)) {
          return convertAvroPrimitiveTypes(TypeName.STRING, value);
        } else {
          throw new RuntimeException("Unknown logical type " + identifier);
        }
      case DECIMAL:
        throw new RuntimeException("Does not support converting DECIMAL type value");
      case MAP:
        throw new RuntimeException("Does not support converting MAP type value");
      default:
        throw new RuntimeException("Does not support converting unknown type value");
    }
  }

  private static ReadableInstant safeToMillis(Object value) {
    long subMilliPrecision = ((long) value) % 1000;
    if (subMilliPrecision != 0) {
      throw new IllegalArgumentException(
          String.format(
              "BigQuery data contained value %s with sub-millisecond precision, which Beam does"
                  + " not currently support.",
              value));
    } else {
      return new Instant((long) value / 1000);
    }
  }

  private static Object convertAvroArray(Field beamField, Object value) {
    // Check whether the type of array element is equal.
    List<Object> values = (List<Object>) value;
    List<Object> ret = new ArrayList();
    for (Object v : values) {
      ret.add(
          convertAvroPrimitiveTypes(
              beamField.getType().getCollectionElementType().getTypeName(), v));
    }
    return (Object) ret;
  }

  private static Object convertAvroString(Object value) {
    if (value instanceof org.apache.avro.util.Utf8) {
      return ((org.apache.avro.util.Utf8) value).toString();
    } else if (value instanceof String) {
      return value;
    } else {
      throw new RuntimeException(
          "Does not support converting avro format: " + value.getClass().getName());
    }
  }

  private static Object convertAvroPrimitiveTypes(TypeName beamType, Object value) {
    switch (beamType) {
      case BYTE:
        return ((Long) value).byteValue();
      case INT16:
        return ((Long) value).shortValue();
      case INT32:
        return ((Long) value).intValue();
      case INT64:
        return value;
      case FLOAT:
        return ((Double) value).floatValue();
      case DOUBLE:
        return (Double) value;
      case BOOLEAN:
        return (Boolean) value;
      case DECIMAL:
        throw new RuntimeException("Does not support converting DECIMAL type value");
      case STRING:
        return convertAvroString(value);
      default:
        throw new RuntimeException(beamType + " is not primitive type.");
    }
  }
}
