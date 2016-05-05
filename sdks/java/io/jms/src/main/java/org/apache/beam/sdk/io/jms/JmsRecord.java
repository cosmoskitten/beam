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
package org.apache.beam.sdk.io.jms;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * JmsRecord contains message payload of the record
 * as well as metadata (JMS headers and properties).
 */
public class JmsRecord implements Serializable {

  private final Map<String, Object> headers;
  private final Map<String, Object> properties;
  private final String text;

  public JmsRecord(
      Map<String, Object> headers,
      Map<String, Object> properties,
      String text) {
    this.headers = headers;
    this.properties = properties;
    this.text = text;
  }

  public Map<String, Object> getHeaders() {
    return this.headers;
  }

  public Map<String, Object> getProperties() {
    return this.properties;
  }

  public String getPayload() {
    return this.text;
  }

  @Override
  public int hashCode() {
    return Arrays.deepHashCode(new Object[]{headers, properties, text});
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof JmsRecord) {
      JmsRecord other = (JmsRecord) obj;
      return headers.equals(other.headers)
          && properties.equals(other.properties)
          && text.equals(other.text);
    } else {
      return false;
    }
  }

}
