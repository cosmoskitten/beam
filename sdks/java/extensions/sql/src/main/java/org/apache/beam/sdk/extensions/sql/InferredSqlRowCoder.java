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

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.reflect.InferredRowCoder;

/**
 * SQL-specific utility to create {@link InferredRowCoder}.
 */
@Experimental
public class InferredSqlRowCoder {

  /**
   * Creates a SQL-specific {@link InferredRowCoder} delegating to the {@link SerializableCoder}
   * for encoding the {@link PCollection} elements.
   */
  public static <T> InferredRowCoder<T> of(Class<T> elementType, Coder<T> elementCoder) {
    return InferredRowCoder.of(elementType, elementCoder);
  }

  /**
   * Creates a SQL-specific {@link InferredRowCoder} delegating to the {@code elementCoder}
   * for encoding the {@link PCollection} elements.
   */
  public static <T extends Serializable> InferredRowCoder<T> ofSerializable(Class<T> elementType) {
    return of(elementType, SerializableCoder.of(elementType));
  }
}
