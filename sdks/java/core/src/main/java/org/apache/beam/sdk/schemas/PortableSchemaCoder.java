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
package org.apache.beam.sdk.schemas;

import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.values.Row;

/**
 * A version of SchemaCoder that can only produce/consume Row instances.
 *
 * <p>Implements the beam:coders:row:v1 standard coder while still satisfying the requirement that a
 * PCollection is only considered to have a schema if its coder is an instance of SchemaCoder.
 */
public class PortableSchemaCoder extends SchemaCoder<Row> {
  private PortableSchemaCoder(Schema schema) {
    super(schema, SerializableFunctions.identity(), SerializableFunctions.identity());
  }

  public static PortableSchemaCoder of(Schema schema) {
    return new PortableSchemaCoder(schema);
  }

  @Override
  public int hashCode() {
    return this.getSchema().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PortableSchemaCoder
        && this.getSchema().equals(((PortableSchemaCoder) obj).getSchema());
  }
}
