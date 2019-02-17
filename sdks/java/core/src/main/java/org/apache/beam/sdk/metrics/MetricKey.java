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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.function.ThrowingConsumer;
import org.apache.beam.sdk.metrics.labels.MetricLabels;

/** Metrics are keyed by the step name they are associated with and the name of the metric. */
@Experimental(Kind.METRICS)
@AutoValue
public abstract class MetricKey implements Serializable {

  public abstract MetricName metricName();

  public abstract MetricLabels labels();

  /**
   * The step name that is associated with this metric or Null if none is associated.
   *
   * <p>TODO(ryan): remove this?
   */
  public String stepName() {
    String ptransform = ptransform();
    if (ptransform == null) {
      throw new IllegalArgumentException("Metric doesn't have PTRANSFORM name: " + this.toString());
    }
    return ptransform;
  }

  @Nullable
  public String ptransform() {
    return labels().ptransform();
  }

  @Nullable
  public String pcollection() {
    return labels().pcollection();
  }

  public boolean isUserMetric() {
    return metricName().isUserMetric();
  }

  @Override
  public String toString() {
    return toString(":", "{", ":", "}");
  }

  public <ExceptionT extends Exception> void forEach(
      ThrowingConsumer<ExceptionT, String> ptransform,
      ThrowingConsumer<ExceptionT, String> pcollection)
      throws ExceptionT {
    if (ptransform() != null) {
      ptransform.accept(ptransform());
    } else if (pcollection() != null) {
      pcollection.accept(pcollection());
    } else {
      throw new IllegalStateException(
          "MetricKey doesn't have PTRANSFORM or PCOLLECTION label: " + this);
    }
  }

  public String toString(String delimiter) {
    if (isUserMetric()) {
      return String.join(delimiter, stepName(), metricName().namespace(), metricName().name());
    }
    if (ptransform() != null) {
      return String.join(delimiter, ptransform(), metricName().toString(delimiter));
    }
    if (pcollection() != null) {
      return String.join(delimiter, pcollection(), metricName().toString(delimiter));
    }
    return toString(delimiter, delimiter, delimiter, "");
  }

  /**
   * Customizable string-representation of a {@link MetricKey}.
   *
   * <p>Defaults to "[namespace]:[name]{PTRANSFORM:[transform name]}":
   *
   * <p>- {@code urnDelimiter} is the ":" between "[namespace]" and "[name]" - {@code openLabels}
   * and {@code closeLabels} are the "{" and "}", resp., which bracket the labels associated with
   * this metric (currently always ["PTRANSFORM"] xor ["PCOLLECTION"]. - {@code labelKVDelimiter} is
   * the ":" between "PTRANSFORM" and "[transform name]"
   */
  public String toString(
      String urnDelimiter, String openLabels, String labelKVDelimiter, String closeLabels) {
    StringBuilder sb = new StringBuilder();
    sb.append(metricName().toString(urnDelimiter));
    sb.append(labels().toString(openLabels, labelKVDelimiter, closeLabels));
    return sb.toString();
  }

  public static MetricKey of(MetricName metricName, MetricLabels labels) {
    return new AutoValue_MetricKey(metricName, labels);
  }

  public static MetricKey ptransform(String ptransform, MetricName name) {
    return new AutoValue_MetricKey(name, MetricLabels.ptransform(ptransform));
  }

  public static MetricKey ptransform(String ptransform, String namespace, String name) {
    return new AutoValue_MetricKey(
        MetricName.named(namespace, name), MetricLabels.ptransform(ptransform));
  }

  public static MetricKey ptransform(String ptransform, Class<?> namespace, String name) {
    return new AutoValue_MetricKey(
        MetricName.named(namespace, name), MetricLabels.ptransform(ptransform));
  }

  public static MetricKey of(MonitoringInfo monitoringInfo) {
    return new AutoValue_MetricKey(
        MetricName.of(monitoringInfo.getUrn()), MetricLabels.create(monitoringInfo));
  }
}
