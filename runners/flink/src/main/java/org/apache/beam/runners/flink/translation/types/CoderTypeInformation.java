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
package org.apache.beam.runners.flink.translation.types;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.coders.Coder;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * Flink {@link org.apache.flink.api.common.typeinfo.TypeInformation} for Beam {@link
 * org.apache.beam.sdk.coders.Coder}s.
 */
public class CoderTypeInformation<T> extends TypeInformation<T> implements AtomicType<T> {

  private final Coder<T> coder;

  public CoderTypeInformation(Coder<T> coder) {
    checkNotNull(coder);
    this.coder = coder;
  }

  public Coder<T> getCoder() {
    return coder;
  }

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public int getArity() {
    return 1;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Class<T> getTypeClass() {
    return (Class<T>) coder.getEncodedTypeDescriptor().getRawType();
  }

  @Override
  public boolean isKeyType() {
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public TypeSerializer<T> createSerializer(ExecutionConfig config) {
    return new CoderTypeSerializer<>(coder);
  }

  @Override
  public int getTotalFields() {
    return 2;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CoderTypeInformation that = (CoderTypeInformation) o;

    return coder.equals(that.coder);
  }

  @Override
  public int hashCode() {
    return coder.hashCode();
  }

  @Override
  public boolean canEqual(Object obj) {
    return obj instanceof CoderTypeInformation;
  }

  @Override
  public String toString() {
    return "CoderTypeInformation{coder=" + coder + '}';
  }

  @Override
  public TypeComparator<T> createComparator(
      boolean sortOrderAscending, ExecutionConfig executionConfig) {
    throw new UnsupportedOperationException("Non-encoded values cannot be compared directly.");
  }
}
