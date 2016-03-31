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
package com.google.cloud.dataflow.sdk.util.common;

import com.google.common.base.Strings;

import javax.annotation.Nullable;

/**
 * A structured counter name.
 *
 * <p>It holds all information for generating structured name protos.
 * It is also used as a wrapper during the migration to structured counters.
 *
 * <p>Before migration, the {@link CounterNameAndMetadata} will be converted to a flat name.
 * During migration, the {@link CounterNameAndMetadata} will be mapped to a structured name proto.
 */
public class CounterNameAndMetadata {
  /**
   * Returns a CounterName with the given name.
   */
  public static CounterNameAndMetadata named(String name) {
    return new CounterNameAndMetadata(name, "", "", "");
  }

  /**
   * Returns a msecs CounterName.
   */
  public static CounterNameAndMetadata msecs(String name) {
    return named(name + "-msecs");
  }

  /**
   * Returns a CounterName identical to this, but with the given origin.
   */
  public CounterNameAndMetadata withOrigin(String origin) {
    return new CounterNameAndMetadata(this.name, origin, this.stepName, this.prefix);
  }

  /**
   * Returns a CounterName identical to this, but with the given original step name.
   */
  public CounterNameAndMetadata withOriginalStepName(String originalStepName) {
    return new CounterNameAndMetadata(this.name, this.origin, originalStepName, this.prefix);
  }

  /**
   * Returns a CounterName identical to this, but with the given prefix.
   */
  public CounterNameAndMetadata withPrefix(String prefix) {
    return new CounterNameAndMetadata(this.name, this.origin, this.stepName, prefix);
  }

  /**
   * Name of the counter.
   *
   * <p>For example, process-msecs, ElementCount.
   */
  private final String name;

  /**
   * Origin (namespace) of counter name.
   *
   * <p>For example, "user" for user-defined counters.
   * It may be null or empty for counters defined by the Dataflow service or SDK.
   */
  @Nullable
  private final String origin;

  /**
   * System defined step name.
   *
   * <p>For example, s1, s2.out.
   * It may be null or empty when counters don't associate with step names.
   */
  @Nullable
  private final String stepName;

  /**
   * Prefix of group of counters.
   *
   * <p>It is null or empty when counters don't have general prefixes.
   */
  @Nullable
  private final String prefix;

  /**
   * Flat name is the equivalent unstructured name.
   *
   * <p>It is null before {@link #getFlatName()} is called.
   */
  @Nullable
  private String flatName;

  private CounterNameAndMetadata(String name, String origin, String stepName, String prefix) {
    this.name = name;
    this.origin = origin;
    this.stepName = stepName;
    this.prefix = prefix;
    this.flatName = null;
  }

  /**
   * Returns the flat name of a structured counter.
   */
  public synchronized String getFlatName() {
    if (flatName == null) {
      StringBuilder sb = new StringBuilder();
      if (!Strings.isNullOrEmpty(prefix)) {
        // Flume doesn't explicitly use "-" to concatenate prefix, it may already have it in it.
        sb.append(prefix);
      }
      if (!Strings.isNullOrEmpty(origin)) {
        sb.append(origin + "-");
      }
      if (!Strings.isNullOrEmpty(stepName)) {
        sb.append(stepName + "-");
      }
      sb.append(name);
      flatName = sb.toString();
    }
    return flatName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o instanceof CounterNameAndMetadata) {
      CounterNameAndMetadata that = (CounterNameAndMetadata) o;
      return this.getFlatName().equals(that.getFlatName());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getFlatName().hashCode();
  }
}
