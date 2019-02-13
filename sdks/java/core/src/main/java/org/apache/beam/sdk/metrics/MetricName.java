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
package org.apache.beam.sdk.metrics;

import static org.apache.beam.sdk.metrics.MetricUrns.USER_METRIC_URN_PREFIX;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * Wrapper for {@link org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo} URN.
 *
 * <p>"User" metrics (URN {@link MetricUrns#USER_METRIC_URN_PREFIX}) are defined by a "namespace"
 * and "name", and are the most commonly dealt with by user code, so structured constructors and
 * accessors are provided in terms of those strings.
 *
 * <p>The {@link #namespace} allows grouping related metrics together and also prevents collisions
 * between multiple metrics with the same name.
 */
@Experimental(Kind.METRICS)
@AutoValue
public abstract class MetricName implements Serializable {

  public abstract String urn();

  @Nullable private String name;

  @Nullable private String namespace;

  /** Parse the urn field into "namespace" and "name" fields. */
  private void parseUrn() {
    String urn = urn();
    if (urn.startsWith(USER_METRIC_URN_PREFIX)) {
      List<String> split = new ArrayList<String>(Arrays.asList(urn.split(":")));
      this.name = split.get(split.size() - 1);
      this.namespace = split.get(split.size() - 2);
    }
  }

  public Boolean isUserMetric() {
    if (this.namespace == null) {
      parseUrn();
    }
    return this.namespace != null;
  }

  /** @return the parsed namespace from the user metric URN, otherwise null. */
  public String getNamespace() {
    if (this.namespace == null) {
      parseUrn();
    }
    checkAccess();
    return this.namespace;
  }
  /** @return the parsed name from the user metric URN, otherwise null. */
  public String getName() {
    if (this.name == null) {
      parseUrn();
    }
    checkAccess();
    return this.name;
  }

  public void checkAccess() {
    if (this.namespace == null || this.name == null) {
      throw new IllegalStateException(
          String.format("Asking for name of a nameless MonitoringInfo metric): %s", urn()));
    }
  }

  @Override
  public String toString() {
    return toString(":");
  }

  public String toString(String delimiter) {
    return String.format("%s%s%s", getNamespace(), delimiter, getName());
  }

  public static MetricName of(String urn) {
    return new AutoValue_MetricName(urn);
  }

  public static MetricName named(String namespace, String name) {
    return new AutoValue_MetricName(MetricUrns.urn(namespace, name));
  }

  public static MetricName named(Class<?> namespace, String name) {
    return new AutoValue_MetricName(MetricUrns.urn(namespace.getName(), name));
  }
}
