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

package org.apache.beam.sdk.nexmark.model.sql;

import java.util.Map;

import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.nexmark.model.sql.adapter.ModelFieldsAdapter;
import org.apache.beam.sdk.nexmark.model.sql.adapter.ModelFieldsAdapters;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.BeamRecord;

/**
 * Convert Java model object to BeamRecord.
 */
public class ToBeamRecord {

  static final ToBeamRecord INSTANCE = new ToBeamRecord(ModelFieldsAdapters.ADAPTERS);

  private Map<Class, ModelFieldsAdapter> modelTypeAdapters;

  private ToBeamRecord(Map<Class, ModelFieldsAdapter> modelTypeAdapters) {
    this.modelTypeAdapters = modelTypeAdapters;
  }

  private BeamRecord toRecord(Object model) {
    if (model == null) {
      return null;
    }

    Class modelClass = model.getClass();

    if (!modelTypeAdapters.containsKey(modelClass)) {
      throw new IllegalArgumentException(
          "Beam SQL record type is not registered for " + model.getClass().getSimpleName());
    }

    ModelFieldsAdapter adapter = modelTypeAdapters.get(modelClass);
    return new BeamRecord(adapter.getRecordType(), adapter.getFieldsValues(model));
  }

  public static ParDo.SingleOutput<KnownSize, BeamRecord> parDo() {
    return ParDo.of(new DoFn<KnownSize, BeamRecord>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        BeamRecord beamRecord = INSTANCE.toRecord(c.element());
        c.output(beamRecord);
      }
    });
  }
}
