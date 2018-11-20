package org.apache.beam.runners.core.metrics;

import java.util.Collections;
import java.util.HashMap;
import java.time.Instant;

import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.core.construction.BeamUrns;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoUrns;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoSpec;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoSpecs;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoTypeUrns;


public class SimpleMonitoringInfoBuilder {
  public static final String ELEMENT_COUNT_URN = BeamUrns.getUrn(MonitoringInfoUrns.Enum.ELEMENT_COUNT);
  public static final String START_BUNDLE_MSECS_URN = BeamUrns.getUrn(MonitoringInfoUrns.Enum.START_BUNDLE_MSECS);
  public static final String PROCESS_BUNDLE_MSECS_URN = BeamUrns.getUrn(MonitoringInfoUrns.Enum.PROCESS_BUNDLE_MSECS);
  public static final String FINISH_BUNDLE_MSECS_URN = BeamUrns.getUrn(MonitoringInfoUrns.Enum.FINISH_BUNDLE_MSECS);
  public static final String TOTAL_MSECS_URN = BeamUrns.getUrn(MonitoringInfoUrns.Enum.TOTAL_MSECS);
  public static final String USER_COUNTER_URN_PREFIX = BeamUrns.getUrn(MonitoringInfoUrns.Enum.USER_COUNTER_URN_PREFIX);

  public static final String SUM_INT64_TYPE = BeamUrns.getUrn(
      MonitoringInfoTypeUrns.Enum.SUM_INT64_TYPE);

  private final static HashMap<String, MonitoringInfoSpec> specs =
      new HashMap<String, MonitoringInfoSpec>();

  private final boolean validateAndDropInvalid;

  static {
    // TODO don't construct this whole object every time
    // Share this as a static ob
    for (MonitoringInfoSpecs.Enum val : MonitoringInfoSpecs.Enum.values()) {
      // Ignore the UNRECOGNIZED = -1 value;
      if (!((Enum) val).name().equals("UNRECOGNIZED")) {
        MonitoringInfoSpec spec = val.getValueDescriptor().getOptions().getExtension(
            BeamFnApi.monitoringInfoSpec);
        SimpleMonitoringInfoBuilder.specs.put(spec.getUrn(), spec);
      }
    }
  }

  public SimpleMonitoringInfoBuilder() {
    this(true);
  }

  public SimpleMonitoringInfoBuilder(boolean validateAndDropInvalid) {
    this.builder = MonitoringInfo.newBuilder();
    this.validateAndDropInvalid = validateAndDropInvalid;
  }

  public boolean validate() {
    String urn = this.builder.getUrn();
    if (urn == null || urn.isEmpty()) {
      // TODO log
      return false;
    }

    MonitoringInfoSpec spec;
    // If it's a user counter, and it has this prefix.
    if (urn.startsWith(USER_COUNTER_URN_PREFIX)) {
      // TODO validate this format, check that it has a namespace and key.
      spec = SimpleMonitoringInfoBuilder.specs.get(USER_COUNTER_URN_PREFIX);
    } else if (!SimpleMonitoringInfoBuilder.specs.containsKey(urn)) {
      // Succeed for unknown URNs, this is an extensible metric.
      // TODO fail if in the beam namespace?
      return true;
    } else {
      spec = SimpleMonitoringInfoBuilder.specs.get(urn);
    }

    if (!this.builder.getType().equals(spec.getTypeUrn())) {
      // TODO log
      return false;
    }

    // TODO less string comparisons.
    // TODO should we just omit this one since the builder enforces this?
    if (spec.getTypeUrn().equals(SUM_INT64_TYPE)) {
      //if (!this.builder.getMetricBuilder().getCounterDataBuilder().hasInt64Value()) {
      //  return false;
      //}
    }

    // based on the spec type make sure the right field is set.
    // TODO faster way to iterate/compare this?
    Set<String> requiredLabels = new HashSet<String>(spec.getRequiredLabelsList());
    //Collections.addAll(requiredLabels, spec.getRequiredLabelsList());

    if (this.builder.getLabels().equals(requiredLabels)) {
      return false;
    }
    return true;
  }


  /**
   * @param namespace The namespace of the metric.
   * @param name The name of the metric.
   * @return The metric URN for a user metric, with a proper URN prefix.
   */
  private static String userMetricUrn(String namespace, String name) {
    StringBuilder sb = new StringBuilder();
    sb.append(USER_COUNTER_URN_PREFIX);
    sb.append(namespace);
    sb.append(':');
    sb.append(name);
    return sb.toString();
  }

  private MonitoringInfo.Builder builder;

  public void setUrn(String urn) {
    this.builder.setUrn(urn);
  }

  public void setUrnForUserMetric(String namespace, String name) {
    this.builder.setUrn(userMetricUrn(namespace, name));
  }

  public void setTimestampToNow() {
    Instant time = Instant.now();
    this.builder.getTimestampBuilder()
        .setSeconds(time.getEpochSecond())
        .setNanos(time.getNano());
  }

  public void setInt64Value(long value) {
    this.builder.getMetricBuilder().getCounterDataBuilder().setInt64Value(value);
    this.builder.setType(SUM_INT64_TYPE);
  }

  public void setPTransformLabel(String pTransform) {
    this.builder.putLabels("PTRANSFORM", pTransform);
  }

  public void setPCollectionLabel(String pCollection) {
    this.builder.putLabels("PCOLLECTION", pCollection);
  }

  public MonitoringInfo build() {
    if (validateAndDropInvalid && !validate()) {
      return null;
    }
    return this.builder.build();
  } // TODO add validation

}
