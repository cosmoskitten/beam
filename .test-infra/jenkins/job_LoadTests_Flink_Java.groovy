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

import CommonJobProperties as commonJobProperties
import CommonTestProperties
import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder
import TestingInfra as infrastructure

def loadTestConfigurations = [
        [
                title        : 'Load test: 2GB of 10B records',
                itClass      : 'org.apache.beam.sdk.loadtests.GroupByKeyLoadTest',
                runner       : CommonTestProperties.Runner.FLINK,
                jobProperties: [
                        project             : 'apache-beam-testing',
                        appName             : 'load_tests_Java_Dataflow_Batch_GBK_1',
                        tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publishToBigQuery   : true,
                        bigQueryDataset     : 'load_test',
                        bigQueryTable       : 'java_dataflow_batch_GBK_1',
                        sourceOptions       : """
                                            {
                                              "numRecords": 200000000,
                                              "keySizeBytes": 1,
                                              "valueSizeBytes": 9
                                            }
                                       """.trim().replaceAll("\\s", ""),
                        fanout              : 1,
                        iterations          : 1,
                        parallelism         : 5,
                        flinkMaster         : 'localhost:8081',
                        autoscalingAlgorithm: "NONE"
                ]
        ]
]

def loadTestJob = { scope, triggeringContext ->
  scope.description('Runs Java GBK load tests on Flink runner in batch mode')
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

  scope.environmentVariables(

          // TOOD: I think the cluster name should be unique here
          CLUSTER_NAME: 'flink',
          GCS_BUCKET: 'gs://temp-storage-for-perf-tests/flink-dataproc-cluster',

          // TODO: download harness images
          HARNESS_IMAGES_TO_PULL: '',
          FLINK_DOWNLOAD_URL: 'http://archive.apache.org/dist/flink/flink-1.5.6/flink-1.5.6-bin-hadoop28-scala_2.11.tgz',
          FLINK_NUM_WORKERS: '5',
          TASK_MANAGER_SLOTS: '1',
          DETACHED_MODE: 'true'
  )

  infrastructure.setupFlinkCluster scope

  for (testConfiguration in loadTestConfigurations) {
    loadTestsBuilder.loadTest(scope, testConfiguration.title, testConfiguration.runner, CommonTestProperties.SDK.JAVA, testConfiguration.jobProperties, testConfiguration.itClass, triggeringContext)
  }

  infrastructure.teardownDataproc scope
}


PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_LoadTests_Java_GBK_Flink_Batch',
        'Run Load Tests Java GBK Flink Batch',
        'Load Tests Java GBK Flink Batch suite',
        this
) {
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}


