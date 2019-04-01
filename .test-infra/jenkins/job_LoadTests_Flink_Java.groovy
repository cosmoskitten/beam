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

def testConfiguration = [
        title        : 'GroupByKey Python load test Direct',
        itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
        runner       : CommonTestProperties.Runner.FLINK,
        sdk          : CommonTestProperties.SDK.PYTHON,
        jobProperties: [
                publish_to_big_query: true,
                project             : 'apache-beam-testing',
                metrics_dataset     : 'load_test_SMOKE',
                metrics_table       : 'python_flink_direct_gbk',
                input_options       : '\'{"num_records": 100000,' +
                        '"key_size": 1,' +
                        '"value_size":1,' +
                        '"bundle_size_distribution_type": "const",' +
                        '"bundle_size_distribution_param": 1,' +
                        '"force_initial_num_bundles": 10}\''
        ]
]

def jobName = 'beam_LoadTests_Python_GBK_Flink_Batch'

PhraseTriggeringPostCommitBuilder.postCommitJob(
        jobName,
        'Run Load Tests Python GBK Flink Batch',
        'Load Tests Python GBK Flink Batch suite',
        this
) {
  description('Runs Java GBK load tests on Flink runner in batch mode')
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

  String repositoryRoot = 'gcr.io/apache-beam-testing/beam_portability'

  String pythonHarnessImage = infrastructure.prepareSDKHarness(scope, testConfiguration.sdk, repositoryRoot, 'latest')
  testConfiguration.jobProperties.environmentType = 'DOCKER'
  testConfiguration.jobProperties.environmentConfig = pythonHarnessImage
  testConfiguration.jobProperties.jobEndpoint = 'localhost:8099'

  String jobServerImage = infrastructure.prepareFlinkJobServer(scope, repositoryRoot, 'latest')
  infrastructure.setupFlinkCluster(scope, jobName, 2, pythonHarnessImage, jobServerImage)

  loadTestsBuilder.loadTest(delegate, testConfiguration.title, testConfiguration.runner, testConfiguration.sdk, testConfiguration.jobProperties, testConfiguration.itClass, CommonTestProperties.TriggeringContext.PR)

  infrastructure.teardownDataproc(scope, jobName)
}
