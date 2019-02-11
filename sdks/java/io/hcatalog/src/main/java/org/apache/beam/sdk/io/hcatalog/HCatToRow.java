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
package org.apache.beam.sdk.io.hcatalog;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hive.hcatalog.data.HCatRecord;

/** HCatToRow. */
public class HCatToRow {

  public static PTransform<PCollection<? extends HCatRecord>, PCollection<Row>> forSchema(
      Schema schema) {
    return ParDo.of(new HCatToRowFn(schema));
  }

  public static PTransform<PBegin, PCollection<Row>> fromSpec(HCatalogIO.Read readSpec) {
    return new PTransform<PBegin, PCollection<Row>>() {
      @Override
      public PCollection<Row> expand(PBegin input) {
        HCatalogBeamSchema hcatSchema = HCatalogBeamSchema.create(readSpec.getConfigProperties());
        Schema schema =
            hcatSchema.getTableSchema(readSpec.getDatabase(), readSpec.getTable()).get();
        return input
            .apply("ReadHCatRecords", readSpec)
            .apply("ConvertToRows", forSchema(schema))
            .setRowSchema(schema);
      }
    };
  }

  static class HCatToRowFn extends DoFn<HCatRecord, Row> {
    private Schema schema;

    HCatToRowFn(Schema schema) {
      this.schema = schema;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      HCatRecord hCatRecord = c.element();
      c.output(Row.withSchema(schema).addValues(hCatRecord.getAll()).build());
    }
  }
}
