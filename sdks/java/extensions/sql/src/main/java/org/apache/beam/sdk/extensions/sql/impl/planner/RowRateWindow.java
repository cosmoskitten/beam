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

import java.util.Objects;

/** This is a utility class to represent rowCount, rate and window. */
public class RowRateWindow {
  private final double window;
  private final double rate;
  private final double rowCount;

  public static final RowRateWindow UNKNOWN =
      new RowRateWindow(
          Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);

  public RowRateWindow(double rowCount, double rate, double window) {
    if (window < 0 || rate < 0 || rowCount < 0) {
      throw new IllegalArgumentException("All the estimates in RowRateWindow should be positive");
    }
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

  public RowRateWindow multiply(double factor) {
    return new RowRateWindow(rowCount * factor, rate * factor, window * factor);
  }

  public RowRateWindow plus(RowRateWindow that) {
    if (this.isUnknown() || that.isUnknown()) {
      return UNKNOWN;
    }
    return new RowRateWindow(
        this.getRowCount() + that.getRowCount(),
        this.getRate() + that.getRate(),
        this.getWindow() + that.getWindow());
  }

  public RowRateWindow minus(RowRateWindow that) {
    if (this.isUnknown() || that.isUnknown()) {
      return UNKNOWN;
    }
    return new RowRateWindow(
        Math.max(this.getRowCount() - that.getRowCount(), 0),
        Math.max(this.getRate() - that.getRate(), 0),
        Math.max(this.getWindow() - that.getWindow(), 0));
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof RowRateWindow)) {
      return false;
    }
    return ((RowRateWindow) obj).rate == this.rate
        && ((RowRateWindow) obj).rowCount == this.rowCount
        && ((RowRateWindow) obj).window == this.window;
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowCount, rate, window);
  }
}
