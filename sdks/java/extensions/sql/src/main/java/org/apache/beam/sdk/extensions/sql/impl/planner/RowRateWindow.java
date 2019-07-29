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
package org.apache.beam.sdk.extensions.sql.impl.planner;

/** This is a utility class to represent rowCount, rate and window. */
public class RowRateWindow {
  private final double window;
  private final double rate;
  private final double rowCount;

  public static final RowRateWindow UNKNOWN =
      new RowRateWindow(
          Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);

  public RowRateWindow(double rowCount, double window, double rate) {
    this.window = window;
    this.rate = rate;
    this.rowCount = rowCount;
  }

  public double getWindow() {
    return window;
  }

  public double getRate() {
    return rate;
  }

  public double getRowCount() {
    return rowCount;
  }

  public boolean isUnknown() {
    return Double.isInfinite(rowCount) || Double.isInfinite(rate) || Double.isInfinite(window);
  }
}
