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
package org.apache.beam.sdk.coders;

import org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects;
import java.util.Objects;

/** A Pojo at the top level for use in tests. */
class AvroCoderTestPojo {

  public String text;

  // Empty constructor required for Avro decoding.
  @SuppressWarnings("unused")
  public AvroCoderTestPojo() {}

  public AvroCoderTestPojo(String text) {
    this.text = text;
  }

  @Override
  public boolean equals(Object other) {
    return (other instanceof AvroCoderTestPojo) && ((AvroCoderTestPojo) other).text.equals(text);
  }

  @Override
  public int hashCode() {
    return Objects.hash(AvroCoderTestPojo.class, text);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("text", text).toString();
  }
}
