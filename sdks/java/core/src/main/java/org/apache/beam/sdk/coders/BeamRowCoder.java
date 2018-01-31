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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.values.BeamRow;
import org.apache.beam.sdk.values.BeamRowType;

/**
 *  A {@link Coder} for {@link BeamRow}. It wraps the {@link Coder} for each element directly.
 */
@Experimental
public class BeamRowCoder extends CustomCoder<BeamRow> {
  private static final BitSetCoder nullListCoder = BitSetCoder.of();

  private BeamRowType recordType;
  private List<Coder> coders;

  private BeamRowCoder(BeamRowType recordType, List<Coder> coders) {
    this.recordType = recordType;
    this.coders = coders;
  }

  public static BeamRowCoder of(BeamRowType recordType, List<Coder> coderArray){
    if (recordType.getFieldCount() != coderArray.size()) {
      throw new IllegalArgumentException("Coder size doesn't match with field size");
    }
    return new BeamRowCoder(recordType, coderArray);
  }

  public BeamRowType getRecordType() {
    return recordType;
  }

  @Override
  public void encode(BeamRow value, OutputStream outStream)
      throws CoderException, IOException {
    nullListCoder.encode(scanNullFields(value), outStream);
    for (int idx = 0; idx < value.getFieldCount(); ++idx) {
      if (value.getFieldValue(idx) == null) {
        continue;
      }

      coders.get(idx).encode(value.getFieldValue(idx), outStream);
    }
  }

  @Override
  public BeamRow decode(InputStream inStream) throws CoderException, IOException {
    BitSet nullFields = nullListCoder.decode(inStream);

    List<Object> fieldValues = new ArrayList<>(recordType.getFieldCount());
    for (int idx = 0; idx < recordType.getFieldCount(); ++idx) {
      if (nullFields.get(idx)) {
        fieldValues.add(null);
      } else {
        fieldValues.add(coders.get(idx).decode(inStream));
      }
    }
    BeamRow record = new BeamRow(recordType, fieldValues);

    return record;
  }

  /**
   * Scan {@link BeamRow} to find fields with a NULL value.
   */
  private BitSet scanNullFields(BeamRow record){
    BitSet nullFields = new BitSet(record.getFieldCount());
    for (int idx = 0; idx < record.getFieldCount(); ++idx) {
      if (record.getFieldValue(idx) == null) {
        nullFields.set(idx);
      }
    }
    return nullFields;
  }

  @Override
  public void verifyDeterministic()
      throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {
    for (Coder c : coders) {
      c.verifyDeterministic();
    }
  }

  public List<Coder> getCoders() {
    return Collections.unmodifiableList(coders);
  }
}
