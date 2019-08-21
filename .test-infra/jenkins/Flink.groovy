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

import CommonJobProperties as common
import CommonTestProperties.SDK
import DockerPublisher

class Flink {
  private static final String flinkVersion = '1.7'
  private static final String flinkDownloadUrl = 'https://archive.apache.org/dist/flink/flink-1.7.0/flink-1.7.0-bin-hadoop28-scala_2.11.tgz'
  private static final String FLINK_DIR = '"$WORKSPACE/src/.test-infra/dataproc"'
  private static final String FLINK_SCRIPT = 'flink_cluster.sh'
  private def job
  private String jobName

  private Flink(job, String jobName) {
    this.job = job
    this.jobName = jobName
  }

  /**
   * Creates Flink cluster and specifies cleanup steps.
   *
   * @param job - jenkins job
   * @param jobName - string to be used as a base for cluster name
   * @param sdkHarnessImages - a list of published SDK Harness images tags
   * @param workerCount - the initial number of worker nodes
   * @param slotsPerTaskmanager - the number of slots per Flink task manager
   */
  static Flink setUp(job, String jobName, List<String> sdkHarnessImages, Integer workerCount, Integer slotsPerTaskmanager = 1) {
    DockerPublisher publisher = new DockerPublisher()
    publisher.publish(job, ":runners:flink:${flinkVersion}:job-server-container:docker", 'flink-job-server')

    String jobServerImageTag = publisher.getFullImageName('flink-job-server')
    Flink flink = new Flink(job, jobName)
    flink.setupFlinkCluster(sdkHarnessImages, jobServerImageTag, workerCount, slotsPerTaskmanager)
    flink.addTeardownFlinkStep()

    return flink
  }

  /**
   * Updates the number of worker nodes in a cluster.
   *
   * @param workerCount - the new number of worker nodes in the cluster
   */
  void scaleCluster(Integer workerCount) {
    job.steps {
      shell("echo Changing number of workers to ${workerCount}")
      environmentVariables {
        env("FLINK_NUM_WORKERS", workerCount)
      }
      shell("cd ${FLINK_DIR}; ./${FLINK_SCRIPT} scale")
    }
  }

  private void setupFlinkCluster(List<String> sdkHarnessImages, String jobServerImageTag, Integer workerCount, Integer slotsPerTaskmanager) {
    String gcsBucket = 'gs://beam-flink-cluster'
    String clusterName = getClusterName()
    String artifactsDir = "${gcsBucket}/${clusterName}"
    String imagesToPull = sdkHarnessImages.join(' ')

    job.steps {
      environmentVariables {
        env("GCLOUD_ZONE", "us-central1-a")
        env("CLUSTER_NAME", clusterName)
        env("GCS_BUCKET", gcsBucket)
        env("FLINK_DOWNLOAD_URL", flinkDownloadUrl)
        env("FLINK_NUM_WORKERS", workerCount)
        env("FLINK_TASKMANAGER_SLOTS", slotsPerTaskmanager)
        env("DETACHED_MODE", 'true')

        if(imagesToPull) {
          env("HARNESS_IMAGES_TO_PULL", imagesToPull)
        }

        if(jobServerImageTag) {
          env("JOB_SERVER_IMAGE", jobServerImageTag)
          env("ARTIFACTS_DIR", artifactsDir)
        }
      }

      shell('echo Setting up flink cluster')
      shell("cd ${FLINK_DIR}; ./${FLINK_SCRIPT} create")
    }
  }

  private void addTeardownFlinkStep() {
    job.publishers {
      postBuildScripts {
        steps {
          shell("cd ${FLINK_DIR}; ./${FLINK_SCRIPT} delete")
        }
        onlyIfBuildSucceeds(false)
        onlyIfBuildFails(false)
      }
    }
  }

  private GString getClusterName() {
    return "${jobName.toLowerCase().replace("_", "-")}-\$BUILD_ID"
  }
}
