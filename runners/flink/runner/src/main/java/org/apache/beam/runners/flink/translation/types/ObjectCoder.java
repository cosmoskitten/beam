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

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A Coder that encodes Object.
 */
public class ObjectCoder implements Coder<Object> {

  @JsonCreator
  public static ObjectCoder of() {
    return INSTANCE;
  }

  private static final ObjectCoder INSTANCE = new ObjectCoder();
  private static final TypeDescriptor<Object> TYPE_DESCRIPTOR = new TypeDescriptor<Object>() {};

  @Override
  public void encode(Object value, OutputStream outStream, Context context)
      throws CoderException, IOException {

  }

  @Override
  public Object decode(InputStream inStream, Context context)
      throws CoderException, IOException {
    return null;
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return null;
  }

  @Override
  public CloudObject asCloudObject() {
    return null;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {

  }

  @Override
  public boolean consistentWithEquals() {
    return false;
  }

  @Override
  public Object structuralValue(Object value) throws Exception {
    return null;
  }

  @Override
  public boolean isRegisterByteSizeObserverCheap(Object value, Context context) {
    return false;
  }

  @Override
  public void registerByteSizeObserver(Object value,
                                       ElementByteSizeObserver observer,
                                       Context context) throws Exception {

  }

  @Override
  public String getEncodingId() {
    return null;
  }

  @Override
  public Collection<String> getAllowedEncodings() {
    return null;
  }

  @Override
  public TypeDescriptor<Object> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

}
