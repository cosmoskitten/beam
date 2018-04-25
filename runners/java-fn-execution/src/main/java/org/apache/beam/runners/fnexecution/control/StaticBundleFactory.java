package org.apache.beam.runners.fnexecution.control;

import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;

/**
 * A factory that can create {@link JobBundleFactory JobBundleFactories} from job-scoped resources
 * provided by an operator.
 *
 * <p>This should be a global or singleton resource that caches JobBundleFactories between calls.
 */
public interface StaticBundleFactory {
  JobBundleFactory getOrCreateForJob(JobInfo jobInfo, ArtifactSource artifactSource);
}
