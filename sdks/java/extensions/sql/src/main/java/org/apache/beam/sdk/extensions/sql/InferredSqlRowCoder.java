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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import org.apache.beam.sdk.values.reflect.RowFactory;

/**
 * ReflectiveRowCoder.
 */
public class InferredSqlRowCoder<T> extends CustomCoder<T> {

  private static final RowFactory ROW_FACTORY =
      RowFactory.withRowTypeFactory(SqlRowTypeFactory.instance());

  private final Coder<T> delegateCoder;
  private final RowType rowType;
  private final RowCoder rowCoder;
  private final Class<T> elementType;

  public static <V> InferredSqlRowCoder<V> of(Coder<V> coder, Class<V> elementType) {
    return new InferredSqlRowCoder<>(coder, elementType);
  }

  public static <V extends Serializable> InferredSqlRowCoder<V> ofSerializable(
      Class<V> elementType) {

    return new InferredSqlRowCoder<>(
        SerializableCoder.of(elementType),
        elementType);
  }

  private InferredSqlRowCoder(Coder<T> delegateCoder, Class<T> elementType) {
    this.delegateCoder = delegateCoder;
    this.elementType = elementType;
    this.rowType = ROW_FACTORY.getRowType(elementType);
    this.rowCoder = this.rowType.getRowCoder();
  }

  public RowCoder getRowCoder() {
    return this.rowCoder;
  }

  public RowType getRowType() {
    return this.rowType;
  }

  public Row createRow(T element) {
    return ROW_FACTORY.create(element);
  }

  @Override
  public void encode(T value, OutputStream outStream) throws CoderException, IOException {
    delegateCoder.encode(value, outStream);
  }

  @Override
  public T decode(InputStream inStream) throws CoderException, IOException {
    return delegateCoder.decode(inStream);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    delegateCoder.verifyDeterministic();
  }
}
