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

package org.apache.beam.sdk.extensions.sql.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Iterables;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.BeamRecord;

/**
 * Contain helpers to assert {@link BeamRecord}s.
 */
public class BeamRecordAsserts {

  /**
   * Asserts result contains single record with a double field.
   */
  public static class ScalarDoubleAssert
      implements SerializableFunction<Iterable<BeamRecord>, Void> {

    private double expected;
    private double delta;

    public ScalarDoubleAssert(double expected, double delta) {
      this.expected = expected;
      this.delta = delta;
    }

    @Override
    public Void apply(Iterable<BeamRecord> input) {
      BeamRecord record = Iterables.getOnlyElement(input);
      assertNotNull(record);
      assertEquals(expected, record.getDouble(0), delta);
      return null;
    }
  }

  /**
   * Asserts result contains single record with an int field.
   */
  public static class ScalarIntAssert
      implements SerializableFunction<Iterable<BeamRecord>, Void> {

    private int expected;

    public ScalarIntAssert(int expected) {
      this.expected = expected;
    }

    @Override
    public Void apply(Iterable<BeamRecord> input) {
      BeamRecord record = Iterables.getOnlyElement(input);
      assertNotNull(record);
      assertEquals(expected, (int) record.getInteger(0));
      return null;
    }
  }

  public static ScalarDoubleAssert matchesScalar(double expected, double delta) {
    return new ScalarDoubleAssert(expected, delta);
  }

  public static ScalarIntAssert matchesScalar(int expected) {
    return new ScalarIntAssert(expected);
  }
}
