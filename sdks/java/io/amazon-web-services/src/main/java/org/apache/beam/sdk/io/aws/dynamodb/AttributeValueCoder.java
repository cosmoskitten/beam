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
package org.apache.beam.sdk.io.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/** Provide a mechanism to encode and decode AttributeValue object. */
public class AttributeValueCoder extends AtomicCoder<AttributeValue> implements Serializable {

  /** Data type of each value type in AttributeValue object. */
  private enum AttributeValueType {
    s, // for String
    n, // for Number
    b, // for Byte
    sS, // for List of String
    nS, // for List of Number
    bS, // for List of Byte
    m, // for Map of String and AttributeValue
    l, // for list of AttributeValued
    bOOL, // for Boolean
    nULLValue, // for null
  };

  private static final AttributeValueCoder INSTANCE = new AttributeValueCoder();

  private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();
  private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();
  private static final BooleanCoder BOOLEAN_CODER = BooleanCoder.of();

  private static final ListCoder<String> LIST_STRING_CODER = ListCoder.of(StringUtf8Coder.of());
  private static final ListCoder<byte[]> LIST_BYTE_CODER = ListCoder.of(ByteArrayCoder.of());

  private static final ListCoder<AttributeValue> LIST_CODER =
      ListCoder.of(AttributeValueCoder.of());
  private static final MapCoder<String, AttributeValue> MAP_CODER =
      MapCoder.of(StringUtf8Coder.of(), AttributeValueCoder.of());

  private AttributeValueCoder() {}

  public static AttributeValueCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(AttributeValue value, OutputStream outStream)
      throws CoderException, IOException {

    if (value.getS() != null) {
      STRING_CODER.encode(AttributeValueType.s.toString(), outStream);
      STRING_CODER.encode(value.getS(), outStream);
    } else if (value.getN() != null) {
      STRING_CODER.encode(AttributeValueType.n.toString(), outStream);
      STRING_CODER.encode(value.getN(), outStream);
    } else if (value.getBOOL() != null) {
      STRING_CODER.encode(AttributeValueType.bOOL.toString(), outStream);
      BOOLEAN_CODER.encode(value.getBOOL(), outStream);
    } else if (value.getB() != null) {
      STRING_CODER.encode(AttributeValueType.b.toString(), outStream);
      BYTE_ARRAY_CODER.encode(convertToByteArray(value.getB()), outStream);
    } else if (value.getSS() != null) {
      STRING_CODER.encode(AttributeValueType.sS.toString(), outStream);
      LIST_STRING_CODER.encode(value.getSS(), outStream);
    } else if (value.getNS() != null) {
      STRING_CODER.encode(AttributeValueType.nS.toString(), outStream);
      LIST_STRING_CODER.encode(value.getNS(), outStream);
    } else if (value.getBS() != null) {
      STRING_CODER.encode(AttributeValueType.bS.toString(), outStream);
      LIST_BYTE_CODER.encode(convertToListByteArray(value.getBS()), outStream);
    } else if (value.getL() != null) {
      STRING_CODER.encode(AttributeValueType.l.toString(), outStream);
      LIST_CODER.encode(value.getL(), outStream);
    } else if (value.getM() != null) {
      STRING_CODER.encode(AttributeValueType.m.toString(), outStream);
      MAP_CODER.encode(value.getM(), outStream);
    } else if (value.getNULL() != null) {
      STRING_CODER.encode(AttributeValueType.nULLValue.toString(), outStream);
      BOOLEAN_CODER.encode(value.getNULL(), outStream);
    } else {
      throw new CoderException("Unknown Type");
    }
  }

  @Override
  public AttributeValue decode(InputStream inStream) throws CoderException, IOException {
    AttributeValue attrValue = new AttributeValue();

    String type = STRING_CODER.decode(inStream);
    AttributeValueType attrType = AttributeValueType.valueOf(type);

    switch (attrType) {
      case s:
        attrValue.setS(STRING_CODER.decode(inStream));
        break;
      case n:
        attrValue.setN(STRING_CODER.decode(inStream));
        break;
      case bOOL:
        attrValue.setBOOL(BOOLEAN_CODER.decode(inStream));
        break;
      case b:
        attrValue.setB(ByteBuffer.wrap(BYTE_ARRAY_CODER.decode(inStream)));
        break;
      case sS:
        attrValue.setSS(LIST_STRING_CODER.decode(inStream));
        break;
      case nS:
        attrValue.setNS(LIST_STRING_CODER.decode(inStream));
        break;
      case bS:
        attrValue.setBS(convertToListByteBuffer(LIST_BYTE_CODER.decode(inStream)));
        break;
      case l:
        attrValue.setL(LIST_CODER.decode(inStream));
        break;
      case m:
        attrValue.setM(MAP_CODER.decode(inStream));
        break;
      case nULLValue:
        attrValue.setNULL(BOOLEAN_CODER.decode(inStream));
        break;
      default:
        throw new CoderException("Unknown Type");
    }

    return attrValue;
  }

  private List<byte[]> convertToListByteArray(List<ByteBuffer> listByteBuffer) {
    List<byte[]> listByteArray = new ArrayList<>();
    for (ByteBuffer buff : listByteBuffer) {
      listByteArray.add(convertToByteArray(buff));
    }
    return listByteArray;
  }

  private byte[] convertToByteArray(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    buffer.position(buffer.position() - bytes.length);
    return bytes;
  }

  private List<ByteBuffer> convertToListByteBuffer(List<byte[]> listByteArr) {
    List<ByteBuffer> byteBufferList = new ArrayList<>();
    for (byte[] byteArr : listByteArr) {
      byteBufferList.add(ByteBuffer.wrap(byteArr));
    }
    return byteBufferList;
  }
}
