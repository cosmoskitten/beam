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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;

/**
 * ProtoRow extends the Row and does late materialisation. It hold a reference to the original proto
 * message and has an overlay on each field of the proto message. It doesn't have it's own Coder as
 * it relies on the SchemaCoder to (de)serialize the message over the wire.
 *
 * <p>Each row has a FieldOverlay that handles specific field conversions, as well has special
 * overlays for Well Know Types, Repeatable, Map and Nullable.
 */
@Experimental(Experimental.Kind.SCHEMAS)
class ProtoRow {

  /**
   * Protobuf FieldOverlay is the interface that each implementation needs to implement to handle a
   * specific field types.
   */
  public interface FieldOverlay<ValueT> extends FieldValueGetter<Message, ValueT> {

    //    /** Convert the overlayed field in the Message and convert it to the Row field. */
    //
    //    ValueT get(Message object);

    ValueT convertGetObject(Object object);

    /** Convert the Row field and set it on the overlayed field of the message. */
    void set(Message.Builder object, ValueT value);

    Object convertSetObject(Object value);

    /** Return the Beam Schema Field of this overlayed field. */
    Schema.Field getSchemaField();
  }

  /** Overlay for Protobuf primitive types. Primitive values are just passed through. */
  static class PrimitiveOverlay implements FieldOverlay<Object> {
    protected Descriptors.FieldDescriptor fieldDescriptor;
    private Schema.Field field;

    PrimitiveOverlay(Descriptors.FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
      this.field =
          Schema.Field.of(
              fieldDescriptor.getName(), ProtoSchema.convertType(fieldDescriptor.getType()));
    }

    @Override
    public Object get(Message message) {
      return convertGetObject(message.getField(fieldDescriptor));
    }

    @Override
    public String name() {
      return field.getName();
    }

    @Override
    public Object convertGetObject(Object object) {
      return object;
    }

    @Override
    public void set(Message.Builder message, Object value) {
      message.setField(fieldDescriptor, value);
    }

    @Override
    public Object convertSetObject(Object value) {
      return value;
    }

    @Override
    public Schema.Field getSchemaField() {
      return field;
    }
  }

  /**
   * Overlay for Bytes. Protobuf Bytes are natively represented as ByteStrings that requires special
   * handling for byte[] of size 0.
   */
  static class BytesOverlay extends PrimitiveOverlay {
    BytesOverlay(Descriptors.FieldDescriptor fieldDescriptor) {
      super(fieldDescriptor);
    }

    @Override
    public Object convertGetObject(Object object) {
      // return object;
      return ((ByteString) object).toByteArray();
    }

    @Override
    public void set(Message.Builder message, Object value) {
      if (value != null && ((byte[]) value).length > 0) {
        // Protobuf messages BYTES doesn't like empty bytes?!
        message.setField(fieldDescriptor, convertSetObject(value));
      }
    }

    @Override
    public Object convertSetObject(Object value) {
      if (value != null) {
        return ByteString.copyFrom((byte[]) value);
      }
      return null;
    }
  }

  /**
   * Overlay handler for the Well Known Type "Wrapper". These wrappers make it possible to have
   * nullable primitives.
   */
  static class WrapperOverlay<ValueT> implements FieldOverlay<ValueT> {
    private Descriptors.FieldDescriptor fieldDescriptor;
    private Descriptors.FieldDescriptor valueDescriptor;
    private FieldOverlay value;

    WrapperOverlay(ProtoSchema protoSchema, Descriptors.FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
      this.valueDescriptor = fieldDescriptor.getMessageType().findFieldByName("value");
      this.value = protoSchema.createFieldLayer(valueDescriptor, false);
    }

    @Override
    public ValueT get(Message message) {
      if (message.hasField(fieldDescriptor)) {
        Message wrapper = (Message) message.getField(fieldDescriptor);
        return (ValueT) value.get(wrapper);
      }
      return null;
    }

    @Override
    public String name() {
      return fieldDescriptor.getName();
    }

    @Override
    public ValueT convertGetObject(Object object) {
      return (ValueT) object;
    }

    @Override
    public void set(Message.Builder message, ValueT value) {
      if (value != null) {
        DynamicMessage.Builder builder =
            DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
        this.value.set(builder, value);
        message.setField(fieldDescriptor, builder.build());
      }
    }

    @Override
    public Object convertSetObject(Object value) {
      return value;
    }

    @Override
    public Schema.Field getSchemaField() {
      return Schema.Field.of(
          fieldDescriptor.getName(),
          ProtoSchema.convertType(valueDescriptor.getType()).withNullable(true));
    }
  }

  /**
   * Overlay handler for the Well Known Type "Timestamp". This wrappers converts from a single Row
   * DATETIME and a protobuf "Timestamp" messsage.
   */
  static class TimestampOverlay implements FieldOverlay<Instant> {
    protected Schema.Field field;
    private Descriptors.FieldDescriptor fieldDescriptor;
    private Descriptors.FieldDescriptor secondsDescriptor;
    private Descriptors.FieldDescriptor nanosDescriptor;

    TimestampOverlay(Descriptors.FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
      this.secondsDescriptor = fieldDescriptor.getMessageType().findFieldByName("seconds");
      this.nanosDescriptor = fieldDescriptor.getMessageType().findFieldByName("nanos");
      this.field =
          Schema.Field.of(fieldDescriptor.getName(), Schema.FieldType.DATETIME).withNullable(true);
    }

    @Override
    public Instant get(Message message) {
      if (message.hasField(fieldDescriptor)) {
        Message wrapper = (Message) message.getField(fieldDescriptor);
        return convertGetObject(wrapper);
      }
      return null;
    }

    @Override
    public String name() {
      return field.getName();
    }

    @Override
    public Instant convertGetObject(Object object) {
      Message timestamp = (Message) object;
      return new Instant(
          (Long) timestamp.getField(secondsDescriptor) * 1000
              + (Integer) timestamp.getField(nanosDescriptor) / 1000000);
    }

    @Override
    public void set(Message.Builder message, Instant value) {
      if (value != null) {
        long totalMillis = value.getMillis();
        long seconds = totalMillis / 1000;
        int ns = (int) (totalMillis % 1000 * 1000000);
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(seconds).setNanos(ns).build();
        message.setField(fieldDescriptor, timestamp);
      }
    }

    @Override
    public Object convertSetObject(Object value) {
      return value;
    }

    @Override
    public Schema.Field getSchemaField() {
      return field;
    }
  }

  /** This overlay converts a nested Message into a nested Row. */
  static class MessageOverlay implements FieldOverlay<Object> {
    private final SerializableFunction toRowFunction;
    private final SerializableFunction fromRowFunction;
    private Descriptors.FieldDescriptor fieldDescriptor;
    private Schema.Field schemaField;

    MessageOverlay(ProtoSchema rootProtoSchema, Descriptors.FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;

      ProtoSchema protoSchema =
          new ProtoSchema(
              DynamicMessage.class,
              fieldDescriptor.getMessageType(),
              rootProtoSchema.getDomain(),
              rootProtoSchema.getRegisteredTypeMapping());
      SchemaCoder<Message> schemaCoder = protoSchema.getSchemaCoder();
      toRowFunction = schemaCoder.getToRowFunction();
      fromRowFunction = schemaCoder.getFromRowFunction();
      this.schemaField =
          Schema.Field.of(
              fieldDescriptor.getName(),
              Schema.FieldType.row(protoSchema.getSchema()).withNullable(true));
    }

    @Override
    public Object get(Message message) {
      if (message.hasField(fieldDescriptor)) {
        return convertGetObject(message.getField(fieldDescriptor));
      }
      return null;
    }

    @Override
    public String name() {
      return this.schemaField.getName();
    }

    @Override
    public Object convertGetObject(Object object) {
      return toRowFunction.apply(object);
    }

    @Override
    public void set(Message.Builder message, Object value) {
      if (value != null) {
        message.setField(fieldDescriptor, convertSetObject(value));
      }
    }

    @Override
    public Object convertSetObject(Object value) {
      return fromRowFunction.apply(value);
    }

    @Override
    public Schema.Field getSchemaField() {
      return this.schemaField;
    }
  }

  /**
   * Proto has a well defined way of storing maps, by having a Message with two fields, named "key"
   * and "value" in a repeatable field. This overlay translates between Row.map and the Protobuf
   * map.
   */
  static class MapOverlay implements FieldOverlay<Map> {
    private Descriptors.FieldDescriptor fieldDescriptor;
    private FieldOverlay key;
    private FieldOverlay value;
    private Schema.Field schemaField;

    MapOverlay(ProtoSchema protoSchema, Descriptors.FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
      key =
          protoSchema.createFieldLayer(
              fieldDescriptor.getMessageType().findFieldByName("key"), false);
      value =
          protoSchema.createFieldLayer(
              fieldDescriptor.getMessageType().findFieldByName("value"), false);
      this.schemaField =
          Schema.Field.of(
              fieldDescriptor.getName(),
              Schema.FieldType.map(key.getSchemaField().getType(), value.getSchemaField().getType())
                  .withNullable(true));
    }

    @Override
    public Map get(Message message) {
      List list = (List) message.getField(fieldDescriptor);
      if (list.size() == 0) {
        return null;
      }
      Map rowMap = new HashMap();
      list.forEach(
          entry -> {
            Message entryMessage = (Message) entry;
            rowMap.put(
                key.convertGetObject(
                    entryMessage.getField(fieldDescriptor.getMessageType().findFieldByName("key"))),
                value.convertGetObject(
                    entryMessage.getField(
                        fieldDescriptor.getMessageType().findFieldByName("value"))));
          });
      return rowMap;
    }

    @Override
    public String name() {
      return this.schemaField.getName();
    }

    @Override
    public Map convertGetObject(Object object) {
      throw new RuntimeException("?");
    }

    @Override
    public void set(Message.Builder message, Map map) {
      if (map != null) {
        List messageMap = new ArrayList();
        map.forEach(
            (k, v) -> {
              DynamicMessage.Builder builder =
                  DynamicMessage.newBuilder(fieldDescriptor.getMessageType());
              builder.setField(
                  fieldDescriptor.getMessageType().findFieldByName("key"), key.convertSetObject(k));
              builder.setField(
                  fieldDescriptor.getMessageType().findFieldByName("value"),
                  value.convertSetObject(v));
              messageMap.add(builder.build());
            });
        message.setField(fieldDescriptor, messageMap);
      }
    }

    @Override
    public Object convertSetObject(Object value) {
      return value;
    }

    @Override
    public Schema.Field getSchemaField() {
      return this.schemaField;
    }
  }

  /**
   * This overlay handles repeatable fields. It handles the Array conversion, but delegates the
   * conversion of the individual elements to an embedded overlay.
   */
  static class ArrayOverlay implements FieldOverlay<List> {
    private Descriptors.FieldDescriptor fieldDescriptor;
    private FieldOverlay element;
    private Schema.Field schemaField;

    ArrayOverlay(ProtoSchema protoSchema, Descriptors.FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
      this.element = protoSchema.createFieldLayer(fieldDescriptor, false);
      this.schemaField =
          Schema.Field.of(
              fieldDescriptor.getName(),
              Schema.FieldType.array(element.getSchemaField().getType()).withNullable(true));
    }

    @Override
    public List get(Message message) {
      List list = (List) message.getField(fieldDescriptor);
      if (list.size() == 0) {
        return null;
      }
      List arrayList = new ArrayList<>();
      list.forEach(
          entry -> {
            arrayList.add(element.convertGetObject(entry));
          });
      return arrayList;
    }

    @Override
    public String name() {
      return this.schemaField.getName();
    }

    @Override
    public List convertGetObject(Object object) {
      throw new RuntimeException("?");
    }

    @Override
    public void set(Message.Builder message, List list) {
      if (list != null) {
        List targetList = new ArrayList();
        list.forEach(
            (e) -> {
              targetList.add(element.convertSetObject(e));
            });
        message.setField(fieldDescriptor, targetList);
      }
    }

    @Override
    public Object convertSetObject(Object value) {
      return value;
    }

    @Override
    public Schema.Field getSchemaField() {
      return this.schemaField;
    }
  }

  /** Enum overlay handles the conversion between a string and a ProtoBuf Enum. */
  static class EnumOverlay implements FieldOverlay<Object> {
    private Descriptors.FieldDescriptor fieldDescriptor;
    private Schema.Field field;

    EnumOverlay(Descriptors.FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
      this.field = Schema.Field.of(fieldDescriptor.getName(), Schema.FieldType.STRING);
    }

    @Override
    public Object get(Message message) {
      return convertGetObject(message.getField(fieldDescriptor));
    }

    @Override
    public String name() {
      return field.getName();
    }

    @Override
    public Object convertGetObject(Object in) {
      return in.toString();
    }

    @Override
    public void set(Message.Builder message, Object value) {
      //     builder.setField(fieldDescriptor,
      // convertSetObject(row.getString(fieldDescriptor.getName())));
      message.setField(fieldDescriptor, convertSetObject(value));
    }

    @Override
    public Object convertSetObject(Object value) {
      Descriptors.EnumDescriptor enumType = fieldDescriptor.getEnumType();
      return enumType.findValueByName(value.toString());
    }

    @Override
    public Schema.Field getSchemaField() {
      return field;
    }
  }

  /**
   * This overlay handles nullable fields. If a primitive field needs to be nullable this overlay is
   * wrapped around the original overlay.
   */
  static class NullableOverlay implements FieldOverlay<Object> {
    private Descriptors.FieldDescriptor fieldDescriptor;
    private FieldOverlay fieldOverlay;

    NullableOverlay(Descriptors.FieldDescriptor fieldDescriptor, FieldOverlay fieldOverlay) {
      this.fieldDescriptor = fieldDescriptor;
      this.fieldOverlay = fieldOverlay;
    }

    @Override
    public Object get(Message message) {
      if (message.hasField(fieldDescriptor)) {
        return fieldOverlay.get(message);
      }
      return null;
    }

    @Override
    public String name() {
      return fieldOverlay.getSchemaField().getName();
    }

    @Override
    public Object convertGetObject(Object object) {
      throw new RuntimeException("Value conversion should never be allowed in nullable fields");
    }

    @Override
    public void set(Message.Builder message, Object value) {
      if (value != null) {
        fieldOverlay.set(message, value);
      }
    }

    @Override
    public Object convertSetObject(Object value) {
      throw new RuntimeException("Value conversion should never be allowed in nullable fields");
    }

    @Override
    public Schema.Field getSchemaField() {
      return fieldOverlay.getSchemaField().withNullable(true);
    }
  }
}
