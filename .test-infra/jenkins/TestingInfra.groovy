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

class TestingInfra {

  static void setupFlinkCluster(def context, Integer workerCount) {

    String buildTag = '\$BUILD_TAG'
    String clusterName = "flink-${buildTag}".toLowerCase().replace("_", "-")

    context.steps {
      environmentVariables {
        env("GCS_BUCKET", 'gs://temp-storage-for-perf-tests/flink-dataproc-cluster')
        env("FLINK_DOWNLOAD_URL", 'http://archive.apache.org/dist/flink/flink-1.5.6/flink-1.5.6-bin-hadoop28-scala_2.11.tgz')
        env("FLINK_NUM_WORKERS", workerCount)
        env("DETACHED_MODE", 'true')
        env("CLUSTER_NAME", clusterName)
      }

      shell('echo Setting up flink cluster')
      shell("cd ${common.absolutePath('src/.test-infra/dataproc/')}; ./create_flink_cluster.sh")
    }
  }

  static void teardownDataproc(def context) {
    context.steps {
      shell('gcloud dataproc clusters delete ${CLUSTER_NAME}')
    }
  }
}