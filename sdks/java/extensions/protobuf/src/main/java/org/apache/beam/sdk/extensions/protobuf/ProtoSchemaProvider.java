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
package org.apache.beam.sdk.extensions.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Schema provider for Protobuf messages. The provider is able to handle pre compiled Message file
 * without external help. For Dynamic Messages a Descriptor needs to be registered up front on a
 * specific URN.
 *
 * <p>It's possible to inherit this class for a specific implementation that communicates with an
 * external registry that maps those URN's with Descriptors.
 */
@Experimental(Experimental.Kind.SCHEMAS)
public class ProtoSchemaProvider implements SchemaProvider {

  private Map<String, SchemaCoder> cache = new HashMap<>();
  private Map<String, Class> overlayClasses = new HashMap<>();

  public ProtoSchemaProvider() {}

  private SchemaCoder ensure(Class rawType) {
    String urn = "class:" + rawType.getName();
    SchemaCoder coder = cache.get(urn);
    if (coder == null) {
      return add(urn, rawType);
    }
    return coder;
  }

  private SchemaCoder add(String urn, Class rawType) {
    ProtoSchema protoSchema =
        new ProtoSchema(rawType, ProtobufUtil.getDescriptorForClass(rawType), overlayClasses);
    SchemaCoder<Message> schemaCoder = protoSchema.getSchemaCoder();
    cache.put(urn, schemaCoder);
    return schemaCoder;
  }

  public SchemaCoder add(String urn, Descriptors.Descriptor descriptor) {
    ProtoSchema protoSchema = new ProtoSchema(DynamicMessage.class, descriptor, overlayClasses);
    SchemaCoder<Message> schemaCoder = protoSchema.getSchemaCoder();
    cache.put(urn, schemaCoder);
    return schemaCoder;
  }

  @Override
  public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    return ensure(typeDescriptor.getRawType()).getSchema();
  }

  @Nullable
  @Override
  public <T> SerializableFunction<T, Row> toRowFunction(TypeDescriptor<T> typeDescriptor) {
    return ensure(typeDescriptor.getRawType()).getToRowFunction();
  }

  @Override
  public <T> SerializableFunction<Row, T> fromRowFunction(TypeDescriptor<T> typeDescriptor) {
    return ensure(typeDescriptor.getRawType()).getFromRowFunction();
  }

  public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor, String urn) {
    checkForDynamicType(typeDescriptor);
    return cache.get(urn).getSchema();
  }

  public <T> SerializableFunction<T, Row> toRowFunction(
      TypeDescriptor<T> typeDescriptor, String urn) {
    checkForDynamicType(typeDescriptor);
    return cache.get(urn).getToRowFunction();
  }

  public <T> SerializableFunction<Row, T> fromRowFunction(
      TypeDescriptor<T> typeDescriptor, String urn) {
    checkForDynamicType(typeDescriptor);
    return cache.get(urn).getFromRowFunction();
  }

  private <T> void checkForDynamicType(TypeDescriptor<T> typeDescriptor) {
    if (!typeDescriptor.getRawType().equals(DynamicMessage.class)) {
      throw new RuntimeException("Urn based schema's are only allowed for DynamicMessages");
    }
  }

  public void registerMessageOverlay(String protoMessageFullName, Class overlayClass) {
    overlayClasses.put(protoMessageFullName, overlayClass);
  }
}
