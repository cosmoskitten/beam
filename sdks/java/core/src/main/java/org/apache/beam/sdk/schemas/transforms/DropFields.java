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

package org.apache.beam.sdk.schemas.transforms;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.FieldAccess;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/** A transform to drop fields from a schema.
 *
 * <p>This is the inverse of the {@link Select} transform. A list of fields to drop is specified,
 **/
public class DropFields {
  public static <T> Inner<T> fieldNames(String... fields) {
    return fieldAccess(FieldAccessDescriptor.withFieldNames(fields));
  }

  public static <T> Inner<T> fieldNames(List<String> fields) {
    return fieldAccess(FieldAccessDescriptor.withFieldNames(fields));
  }

  public static <T> Inner<T> fieldIds(Integer... fieldIds) {
    return fieldAccess(FieldAccessDescriptor.withFieldIds(fieldIds));
  }

  public static <T> Inner<T> fieldIds(List<Integer> fields) {
    return fieldAccess(FieldAccessDescriptor.withFieldIds(fields));
  }

  public static <T> Inner<T> fieldAccess(FieldAccessDescriptor fieldsToDrop) {
    return new Inner<>(fieldsToDrop);
  }

  public static class Inner<T> extends PTransform<PCollection<T>, PCollection<Row>> {
    private final FieldAccessDescriptor fieldsToDrop;

    private Inner(FieldAccessDescriptor fieldsToDrop) {
      this.fieldsToDrop = fieldsToDrop;
    }

    FieldAccessDescriptor complement(Schema inputSchema, FieldAccessDescriptor input) {
      Set<String> fieldNamesToSelect = Sets.newHashSet();
      Map<String, FieldAccessDescriptor> nestedFieldsToSelect = Maps.newHashMap();
      for (int i = 0; i < inputSchema.getFieldCount(); ++i) {
        if (input.fieldIdsAccessed().contains(i)) {
          continue;
        }
        Field field = inputSchema.getField(i);
        FieldAccessDescriptor nestedDescriptor = input.nestedFieldsById().get(i);
        if (nestedDescriptor != null) {
          FieldType fieldType = inputSchema.getField(i).getType();
          Preconditions.checkArgument(fieldType.getTypeName().isCompositeType());
          nestedFieldsToSelect.put(field.getName(), complement(fieldType.getRowSchema(), nestedDescriptor));
        } else {
          fieldNamesToSelect.add(field.getName());
        }
      }
      FieldAccessDescriptor fieldAccess = FieldAccessDescriptor.withFieldNames(fieldNamesToSelect);
      for (Map.Entry<String, FieldAccessDescriptor> entry : nestedFieldsToSelect.entrySet()) {
        fieldAccess = fieldAccess.withNestedField(entry.getKey(), entry.getValue());
      }
      return fieldAccess.resolve(inputSchema);
    }

    @Override
    public PCollection<Row> expand(PCollection<T> input) {
      Schema inputSchema = input.getSchema();
      FieldAccessDescriptor selectDescriptor = complement(
          inputSchema, fieldsToDrop.resolve(inputSchema));

      return Select.<T>fieldAccess(selectDescriptor).expand(input);
    }
  }
}