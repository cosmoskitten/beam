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

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class DropFields {
  public static <T> Inner<T> of (String... fields) {
    return of(Arrays.asList(fields));
  }

  public static <T> Inner<T> of(List<String> fields) {
    return new Inner<>(fields);
  }

  public static class Inner<T> extends PTransform<PCollection<T>, PCollection<Row>> {
    private final List<String> fieldsToDrop;

    private Inner(List<String> fieldsToDrop) {
      this.fieldsToDrop = fieldsToDrop;
    }

    @Override
    public PCollection<Row> expand(PCollection<T> input) {
      Schema inputSchema = input.getSchema();

      List<Integer> fieldIdsToDrop = Lists.newArrayList();
      for (String fieldName : fieldsToDrop) {
        fieldIdsToDrop.add(inputSchema.indexOf(fieldName));
      }
      Collections.sort(fieldIdsToDrop);

      Schema.Builder outputSchemaBuilder = Schema.builder();
      int currentDropIndex = 0;
      for (int i = 0; i < input.getSchema().getFieldCount(); ++i) {
        if (currentDropIndex < fieldIdsToDrop.size()
            && i == fieldIdsToDrop.get(currentDropIndex)) {
          ++i;
        } else {
          outputSchemaBuilder.addField(input.getSchema().getField(i));
        }
      }
      final Schema outputSchema = outputSchemaBuilder.build();

      return input.apply(ParDo.of(new DoFn<T, Row>() {
        @ProcessElement
        public void processElement(@Element Row input, OutputReceiver<Row> o) {
          List<Object> values = Lists.newArrayListWithCapacity(outputSchema.getFieldCount());
          int currentDropIndex = 0;
          for (int i = 0; i < input.getSchema().getFieldCount(); ++i) {
            if (currentDropIndex < fieldIdsToDrop.size()
              && i == fieldIdsToDrop.get(currentDropIndex)) {
              ++i;
            } else {
              values.add(input.getValue(i));
            }
          }
          Row newRow = Row.withSchema(outputSchema).attachValues(values).build();
          o.output(newRow);
      }})).setRowSchema(outputSchema);
    }
  }
}