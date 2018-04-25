package org.apache.beam.runners.fnexecution.provisioning;

import com.google.auto.value.AutoValue;
import com.google.protobuf.Struct;

/**
 * A subset of {@link org.apache.beam.model.fnexecution.v1.ProvisionApi.ProvisionInfo} that
 * specifies a unique job, while omitting fields that are not known to the runner operator.
 */
@AutoValue
public abstract class JobInfo {
  public static JobInfo create(String jobId, String jobName, Struct pipelineOptions) {
    return new AutoValue_JobInfo(jobId, jobName, pipelineOptions);
  }

  public abstract String jobId();
  public abstract String jobName();
  public abstract Struct pipelineOptions();
}
