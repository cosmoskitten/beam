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

import static com.google.common.base.MoreObjects.toStringHelper;
import static org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.BooleanCoder;
import static org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.DateCoder;
import static org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.DoubleCoder;
import static org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.FloatCoder;
import static org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.ShortCoder;
import static org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper.TimeCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/**
 * Coders for SQL types supported in Beam.
 *
 * <p>Wraps the underlying {@link AtomicCoder}s instances and adds a URN to them.
 *
 * <p>Relies on URN for equality and hashcode.
 */
public class SqlTypeCoder extends CustomCoder<Object> {

  public static final SqlTypeCoder TINYINT =
      of(ByteCoder.of(), "urn:beam:coders:sql_tinyint:0.1");
  public static final SqlTypeCoder SMALLINT =
      of(ShortCoder.of(), "urn:beam:coders:sql_smallint:0.1");
  public static final SqlTypeCoder INTEGER =
      of(BigEndianIntegerCoder.of(), "urn:beam:coders:sql_integer:0.1");
  public static final SqlTypeCoder BIGINT =
      of(BigEndianLongCoder.of(), "urn:beam:coders:sql_bigint:0.1");
  public static final SqlTypeCoder FLOAT =
      of(FloatCoder.of(), "urn:beam:coders:sql_float:0.1");
  public static final SqlTypeCoder DOUBLE =
      of(DoubleCoder.of(), "urn:beam:coders:sql_double:0.1");
  public static final SqlTypeCoder DECIMAL =
      of(BigDecimalCoder.of(), "urn:beam:coders:sql_decimal:0.1");
  public static final SqlTypeCoder BOOLEAN =
      of(BooleanCoder.of(), "urn:beam:coders:sql_boolean:0.1");
  public static final SqlTypeCoder CHAR =
      of(StringUtf8Coder.of(), "urn:beam:coders:sql_char:0.1");
  public static final SqlTypeCoder VARCHAR =
      of(StringUtf8Coder.of(), "urn:beam:coders:sql_varchar:0.1");
  public static final SqlTypeCoder TIME =
      of(TimeCoder.of(), "urn:beam:coders:sql_time:0.1");
  public static final SqlTypeCoder DATE =
      of(DateCoder.of(), "urn:beam:coders:sql_date:0.1");
  public static final SqlTypeCoder TIMESTAMP =
      of(DateCoder.of(), "urn:beam:coders:sql_timestamp:0.1");

  private final String urn;
  private final AtomicCoder delegateCoder;

  private SqlTypeCoder(AtomicCoder delegateCoder, String urn) {
    this.delegateCoder = delegateCoder;
    this.urn = urn;
  }

  private static SqlTypeCoder of(AtomicCoder delegateCoder, String urn) {
    return new SqlTypeCoder(delegateCoder, urn);
  }

  @Override
  public void encode(Object value, OutputStream outStream) throws CoderException, IOException {
    delegateCoder.encode(value, outStream);
  }

  @Override
  public Object decode(InputStream inStream) throws CoderException, IOException {
    return delegateCoder.decode(inStream);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    delegateCoder.verifyDeterministic();
  }

  public String getUrn() {
    return urn;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof SqlTypeCoder)) {
      return false;
    }

    return Objects.equals(urn, ((SqlTypeCoder) o).urn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(urn);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("urn", urn).toString();
  }
}
