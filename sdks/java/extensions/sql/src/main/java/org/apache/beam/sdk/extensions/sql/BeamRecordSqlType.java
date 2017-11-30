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
package org.apache.beam.sdk.extensions.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.BooleanCoder;
import org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.DateCoder;
import org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.DoubleCoder;
import org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.FloatCoder;
import org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.ShortCoder;
import org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.TimeCoder;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.BeamRecordType;

/**
 * Type provider for {@link BeamRecord} with SQL types.
 *
 * <p>Limited SQL types are supported now, visit
 * <a href="https://beam.apache.org/documentation/dsls/sql/#data-types">data types</a>
 * for more details.
 *
 * <p>This is a SQL specific subclass of {@link BeamRecordType}. In addition to
 * encapsulating field names and coders, this class also contains information about
 * SQL types of the fields. Constants from {@link java.sql.Types} are used to represent the
 * supported SQL types.
 */
@Experimental
public class BeamRecordSqlType extends BeamRecordType {
  private static final ImmutableMap<Integer, Class> JAVA_CLASSES = ImmutableMap
      .<Integer, Class>builder()
      .put(Types.TINYINT, Byte.class)
      .put(Types.SMALLINT, Short.class)
      .put(Types.INTEGER, Integer.class)
      .put(Types.BIGINT, Long.class)
      .put(Types.FLOAT, Float.class)
      .put(Types.DOUBLE, Double.class)
      .put(Types.DECIMAL, BigDecimal.class)
      .put(Types.BOOLEAN, Boolean.class)
      .put(Types.CHAR, String.class)
      .put(Types.VARCHAR, String.class)
      .put(Types.TIME, GregorianCalendar.class)
      .put(Types.DATE, Date.class)
      .put(Types.TIMESTAMP, Date.class)
      .build();

  private static final Map<Integer, Coder> CODERS = ImmutableMap
      .<Integer, Coder>builder()
      .put(Types.TINYINT, ByteCoder.of())
      .put(Types.SMALLINT, ShortCoder.of())
      .put(Types.INTEGER, BigEndianIntegerCoder.of())
      .put(Types.BIGINT, BigEndianLongCoder.of())
      .put(Types.FLOAT, FloatCoder.of())
      .put(Types.DOUBLE, DoubleCoder.of())
      .put(Types.DECIMAL, BigDecimalCoder.of())
      .put(Types.BOOLEAN, BooleanCoder.of())
      .put(Types.CHAR, StringUtf8Coder.of())
      .put(Types.VARCHAR, StringUtf8Coder.of())
      .put(Types.TIME, TimeCoder.of())
      .put(Types.DATE, DateCoder.of())
      .put(Types.TIMESTAMP, DateCoder.of())
      .build();

  public List<Integer> fieldTypes;

  /**
   * Creates a {@link BeamRecordSqlType} from the list of field names and field types.
   *
   * <p>Consider using {@link #builder()} instead of this method for readability.
   *
   * <p>Field types are {@code int} constants from {@link java.sql.Types}.
   *
   * <p>Only subset of SQL types are supported, see
   * <a href="https://beam.apache.org/documentation/dsls/sql/#data-types">data types</a>
   *
   * <p>{@link IllegalStateException} is thrown if {@code fieldNames.size()} does not match
   * {@code fieldTypes.size()}.
   *
   * <p>{@link UnsupportedOperationException} is thrown if the type in {@code fieldTypes}
   * is not supported.
   */
  public static BeamRecordSqlType create(List<String> fieldNames,
                                         List<Integer> fieldTypes) {
    return new BeamRecordSqlType(fieldNames, fieldTypes);
  }

  private BeamRecordSqlType(List<String> fieldNames,
                              List<Integer> fieldTypes) {
    super(fieldNames, toCodersList(fieldTypes));
    this.fieldTypes = fieldTypes;
  }

  private static List<Coder> toCodersList(List<Integer> fieldTypes) {
    List<Coder> fieldCoders = new ArrayList<>(fieldTypes.size());

    for (Integer fieldType : fieldTypes) {
      if (!CODERS.containsKey(fieldType)) {
        throw new UnsupportedOperationException(
                "Data type: " + fieldType + " not supported yet!");
      }

      fieldCoders.add(CODERS.get(fieldType));
    }
    return fieldCoders;
  }

  @Override
  public void validateValueType(int index, Object fieldValue) throws IllegalArgumentException {
    if (null == fieldValue) {// no need to do type check for NULL value
      return;
    }

    int fieldType = fieldTypes.get(index);
    Class javaClazz = JAVA_CLASSES.get(fieldType);
    if (javaClazz == null) {
      throw new IllegalArgumentException("Data type: " + fieldType + " not supported yet!");
    }

    if (!fieldValue.getClass().equals(javaClazz)) {
      throw new IllegalArgumentException(
          String.format("[%s](%s) doesn't match type [%s]",
              fieldValue, fieldValue.getClass(), fieldType)
      );
    }
  }

  /**
   * Returns an unmodifiable list of field types.
   *
   * <p>Types are represented by {@code int} constants from {@link java.sql.Types}.
   *
   * <p>Order of the types in the list matches {@link #getFieldNames()}.
   * Size matches {@link #getFieldCount()}.
   */
  public List<Integer> getFieldTypes() {
    return Collections.unmodifiableList(fieldTypes);
  }

  /**
   * Returns the SQL type of a field. Field is identified by a zero-based {@code index}.
   *
   * <p>Type is an {@code int} constant from {@link java.sql.Types}.
   */
  public Integer getFieldTypeByIndex(int index) {
    return fieldTypes.get(index);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && obj instanceof BeamRecordSqlType) {
      BeamRecordSqlType ins = (BeamRecordSqlType) obj;
      return fieldTypes.equals(ins.getFieldTypes()) && getFieldNames().equals(ins.getFieldNames());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return 31 * getFieldNames().hashCode() + getFieldTypes().hashCode();
  }

  @Override
  public String toString() {
    return "BeamRecordSqlType [fieldNames=" + getFieldNames()
        + ", fieldTypes=" + fieldTypes + "]";
  }

  /**
   * Returns a new {@link Builder} to assist with assembling the {@link BeamRecordSqlType}
   * from the fields.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class to construct {@link BeamRecordSqlType}.
   */
  public static class Builder {

    private ImmutableList.Builder<String> fieldNames;
    private ImmutableList.Builder<Integer> fieldTypes;

    /**
     * Adds a field to the builder.
     *
     * @param fieldName is a name of the field;
     * @param fieldType is a type of the field. One of the supported types from
     *                  {@link java.sql.Types};
     *
     * Will throw {@link UnsupportedOperationException} from {@link #build()}
     * if a field type is not supported.
     */
    public Builder withField(String fieldName, Integer fieldType) {
      fieldNames.add(fieldName);
      fieldTypes.add(fieldType);
      return this;
    }

    public Builder withTinyIntField(String fieldName) {
      return withField(fieldName, Types.TINYINT);
    }

    public Builder withSmallIntField(String fieldName) {
      return withField(fieldName, Types.SMALLINT);
    }

    public Builder withIntegerField(String fieldName) {
      return withField(fieldName, Types.INTEGER);
    }

    public Builder withBigIntField(String fieldName) {
      return withField(fieldName, Types.BIGINT);
    }

    public Builder withFloatField(String fieldName) {
      return withField(fieldName, Types.FLOAT);
    }

    public Builder withDoubleField(String fieldName) {
      return withField(fieldName, Types.DOUBLE);
    }

    public Builder withDecimalField(String fieldName) {
      return withField(fieldName, Types.DECIMAL);
    }

    public Builder withBooleanField(String fieldName) {
      return withField(fieldName, Types.BOOLEAN);
    }

    public Builder withCharField(String fieldName) {
      return withField(fieldName, Types.CHAR);
    }

    public Builder withVarcharField(String fieldName) {
      return withField(fieldName, Types.VARCHAR);
    }

    public Builder withTimeField(String fieldName) {
      return withField(fieldName, Types.TIME);
    }

    public Builder withDateField(String fieldName) {
      return withField(fieldName, Types.DATE);
    }

    public Builder withTimestampField(String fieldName) {
      return withField(fieldName, Types.TIMESTAMP);
    }

    private Builder() {
      this.fieldNames = ImmutableList.builder();
      this.fieldTypes = ImmutableList.builder();
    }

    /**
     * Returns a new instance of {@link BeamRecordSqlType}.
     *
     * <p>Validations are performed similar to {@link BeamRecordSqlType#create(List, List)}.
     */
    public BeamRecordSqlType build() {
      return create(fieldNames.build(), fieldTypes.build());
    }
  }
}
