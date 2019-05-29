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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Factory;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;

/**
 * ProtoSchema is a top level anchor point. It makes sure it can recreate the complete schema and
 * overlay with just the Message raw type or if it's a DynamicMessage with the serialised
 * Descriptor.
 *
 * <ul>
 *   <li>Protobuf oneOf fields are mapped to nullable fields and flattened into the parent row.
 *   <li>Protobuf primitives are mapped to it's nullable counter part.
 *   <li>Protobuf maps are mapped to nullable maps, where empty maps are mapped to the null value.
 *   <li>Protobuf repeatables are mapped to nullable arrays, where empty arrays are mapped to the
 *       null value.
 *   <li>Protobuf enums are mapped to non-nullable string values.
 *   <li>Enum map to their string representation
 * </ul>
 *
 * <p>Protobuf Well Know Types are handled by the Beam Schema system. Beam knows of the following
 * Well Know Types:
 *
 * <ul>
 *   <li>google.protobuf.Timestamp maps to a nullable Field.DATATIME.
 *   <li>google.protobuf.StringValue maps to a nullable Field.STRING.
 *   <li>google.protobuf.DoubleValue maps to a nullable Field.DOUBLE.
 *   <li>google.protobuf.FloatValue maps to a nullable Field.FLOAT.
 *   <li>google.protobuf.BytesValue maps to a nullable Field.BYTES.
 *   <li>google.protobuf.BoolValue maps to a nullable Field.BOOL.
 *   <li>google.protobuf.Int64Value maps to a nullable Field.INT64.
 *   <li>google.protobuf.Int32Value maps to a nullable Field.INT32.
 *   <li>google.protobuf.UInt64Value maps to a nullable Field.INT64.
 *   <li>google.protobuf.UInt32Value maps to a nullable Field.INT32.
 * </ul>
 */
@Experimental(Experimental.Kind.SCHEMAS)
public class ProtoSchema implements Serializable {
  public static final long serialVersionUID = 1L;

  private final Class rawType;
  private final Map<String, Class> typeMapping;
  private final ProtoDomain domain;

  private transient Descriptors.Descriptor descriptor;
  private transient Schema schema;
  private transient Method fnNewBuilder;
  private transient ArrayList<ProtoRow.FieldOverlay> getters;

  private static Map<UUID, ProtoSchema> globalSchemaCache = new HashMap<>();

  ProtoSchema(
      Class rawType,
      Descriptors.Descriptor descriptor,
      ProtoDomain domain,
      Map<String, Class> overlayClasses) {
    this.rawType = rawType;
    this.descriptor = descriptor;
    this.typeMapping = overlayClasses;
    this.domain = domain;
    init();
  }

  public static ProtoSchema fromSchema(Schema schema) {
    return globalSchemaCache.get(schema.getUUID());
  }

  static Schema.FieldType convertType(Descriptors.FieldDescriptor.Type type) {
    switch (type) {
      case DOUBLE:
        return Schema.FieldType.DOUBLE;
      case FLOAT:
        return Schema.FieldType.FLOAT;
      case INT64:
      case UINT64:
      case SINT64:
      case FIXED64:
      case SFIXED64:
        return Schema.FieldType.INT64;
      case INT32:
      case FIXED32:
      case UINT32:
      case SFIXED32:
      case SINT32:
        return Schema.FieldType.INT32;
      case BOOL:
        return Schema.FieldType.BOOLEAN;
      case STRING:
      case ENUM:
        return Schema.FieldType.STRING;
      case BYTES:
        return Schema.FieldType.BYTES;
      case MESSAGE:
      case GROUP:
        break;
    }
    throw new RuntimeException("Field type not matched.");
  }

  private static boolean isMap(Descriptors.FieldDescriptor protoField) {
    return protoField.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
        && protoField.getMessageType().getFullName().endsWith("Entry")
        && (protoField.getMessageType().findFieldByName("key") != null)
        && (protoField.getMessageType().findFieldByName("value") != null);
  }

  ProtoRow.FieldOverlay createFieldLayer(Descriptors.FieldDescriptor protoField, boolean nullable) {
    Descriptors.FieldDescriptor.Type fieldDescriptor = protoField.getType();
    ProtoRow.FieldOverlay fieldOverlay;
    switch (fieldDescriptor) {
      case DOUBLE:
      case FLOAT:
      case INT64:
      case UINT64:
      case SINT64:
      case FIXED64:
      case SFIXED64:
      case INT32:
      case FIXED32:
      case UINT32:
      case SFIXED32:
      case SINT32:
      case BOOL:
      case STRING:
        fieldOverlay = new ProtoRow.PrimitiveOverlay(protoField);
        break;
      case BYTES:
        fieldOverlay = new ProtoRow.BytesOverlay(protoField);
        break;
      case ENUM:
        fieldOverlay = new ProtoRow.EnumOverlay(protoField);
        break;
      case MESSAGE:
        String fullName = protoField.getMessageType().getFullName();
        if (typeMapping.containsKey(fullName)) {
          Class aClass = typeMapping.get(fullName);
          try {
            Constructor constructor = aClass.getConstructor(Descriptors.FieldDescriptor.class);
            return (ProtoRow.FieldOverlay) constructor.newInstance(protoField);
          } catch (NoSuchMethodException e) {
            throw new RuntimeException("Unable to find constructor for Overlay mapper.");
          } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException("Unable to invoke Overlay mapper.");
          }
        }
        switch (fullName) {
          case "google.protobuf.Timestamp":
            return new ProtoRow.TimestampOverlay(protoField);
          case "google.protobuf.StringValue":
          case "google.protobuf.DoubleValue":
          case "google.protobuf.FloatValue":
          case "google.protobuf.BoolValue":
          case "google.protobuf.Int64Value":
          case "google.protobuf.Int32Value":
          case "google.protobuf.UInt64Value":
          case "google.protobuf.UInt32Value":
          case "google.protobuf.BytesValue":
            return new ProtoRow.WrapperOverlay(this, protoField);
          case "google.protobuf.Duration":
          default:
            if (isMap(protoField)) {
              return new ProtoRow.MapOverlay(this, protoField);
            } else {
              return new ProtoRow.MessageOverlay(this, protoField);
            }
        }
      case GROUP:
      default:
        throw new RuntimeException("Field type not matched.");
    }
    if (nullable) {
      return new ProtoRow.NullableOverlay(protoField, fieldOverlay);
    }
    return fieldOverlay;
  }

  private ArrayList<ProtoRow.FieldOverlay> createFieldLayer(Descriptors.Descriptor descriptor) {
    // Oneof fields are nullable, even as they are primitive or enums
    List<Descriptors.FieldDescriptor> oneofMap =
        descriptor.getOneofs().stream()
            .flatMap(oneofDescriptor -> oneofDescriptor.getFields().stream())
            .collect(Collectors.toList());

    ArrayList<ProtoRow.FieldOverlay> fieldOverlays = new ArrayList<>();
    Iterator<Descriptors.FieldDescriptor> protoFields = descriptor.getFields().iterator();
    for (int i = 0; i < descriptor.getFields().size(); i++) {
      Descriptors.FieldDescriptor protoField = protoFields.next();
      if (protoField.isRepeated() && !isMap(protoField)) {
        fieldOverlays.add(new ProtoRow.ArrayOverlay(this, protoField));
      } else {
        fieldOverlays.add(createFieldLayer(protoField, oneofMap.contains(protoField)));
      }
    }
    return fieldOverlays;
  }

  private void init() {
    this.getters = createFieldLayer(descriptor);

    Schema.Builder builder = Schema.builder();
    for (ProtoRow.FieldOverlay field : getters) {
      builder.addField(field.getSchemaField());
    }

    schema = builder.build();
    schema.setUUID(UUID.randomUUID());
    globalSchemaCache.put(schema.getUUID(), this);
    try {
      if (DynamicMessage.class.equals(rawType)) {
        this.fnNewBuilder = rawType.getMethod("newBuilder", Descriptors.Descriptor.class);
      } else {
        this.fnNewBuilder = rawType.getMethod("newBuilder");
      }
    } catch (NoSuchMethodException e) {
    }
  }

  public Schema getSchema() {
    return this.schema;
  }

  SchemaCoder<Message> getSchemaCoder() {
    return SchemaCoder.of(schema, getToRowFunction(), getFromRowFunction());
  }

  private SerializableFunction<Message, Row> getToRowFunction() {
    return new MessageToRowFunction(this);
  }

  private SerializableFunction<Row, Message> getFromRowFunction() {
    return new RowToMessageFunction(this);
  }

  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.defaultWriteObject();
    if (DynamicMessage.class.equals(this.rawType)) {
      if (this.descriptor == null) {
        throw new RuntimeException("DynamicMessages require provider a Descriptor to the coder.");
      }
      oos.writeUTF(descriptor.getFile().getName());
      oos.writeUTF(descriptor.getName());
    }
  }

  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    ois.defaultReadObject();
    if (DynamicMessage.class.equals(rawType)) {
      descriptor = domain.getDescriptor(ois.readUTF(), ois.readUTF());
    } else {
      descriptor = ProtobufUtil.getDescriptorForClass(rawType);
    }
    init();
  }

  public Map<String, Class> getRegisteredTypeMapping() {
    return typeMapping;
  }

  public ProtoDomain getDomain() {
    return domain;
  }

  /** Overlay. */
  public static class ProtoOverlayFactory implements Factory<List<FieldValueGetter>> {

    public ProtoOverlayFactory() {}

    @Override
    public List create(Class<?> clazz, Schema schema) {
      return ProtoSchema.fromSchema(schema).getters;
    }
  }

  private static class MessageToRowFunction implements SerializableFunction<Message, Row> {
    private ProtoSchema protoSchema;

    private MessageToRowFunction(ProtoSchema protoSchema) {
      this.protoSchema = protoSchema;
    }

    @Override
    public Row apply(Message input) {
      return Row.withSchema(protoSchema.schema)
          .withFieldValueGettersHandleCollections(true)
          .withFieldValueGetters(new ProtoOverlayFactory(), input)
          .build();

      // return new ProtoRow(protoSchema.schema, protoSchema.getters, input);
    }
  }

  private static class RowToMessageFunction implements SerializableFunction<Row, Message> {

    private ProtoSchema protoSchema;

    private RowToMessageFunction(ProtoSchema protoSchema) {
      this.protoSchema = protoSchema;
    }

    @Override
    public Message apply(Row input) {
      Message.Builder builder;
      try {
        if (DynamicMessage.class.equals(protoSchema.rawType)) {
          builder =
              (Message.Builder)
                  protoSchema.fnNewBuilder.invoke(protoSchema.rawType, protoSchema.descriptor);
        } else {
          builder = (Message.Builder) protoSchema.fnNewBuilder.invoke(protoSchema.rawType);
        }
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException("Can't invoke newBuilder on the Protobuf message class.", e);
      }

      Iterator values = input.getValues().iterator();
      Iterator<ProtoRow.FieldOverlay> getters = protoSchema.getters.iterator();

      for (int i = 0; i < input.getValues().size(); i++) {
        ProtoRow.FieldOverlay getter = getters.next();
        Object value = values.next();
        getter.set(builder, value);
      }
      return builder.build();
    }
  }
}
