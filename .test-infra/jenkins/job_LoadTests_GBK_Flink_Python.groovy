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
import Flink

String pythonHarnessImageTag = Flink.getSDKHarnessImageTag(CommonTestProperties.SDK.PYTHON)
String now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

int parallelism = 5

def testConfiguration = { datasetName ->
        [
                title        : 'Load test: 2GB of 10B records',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.PORTABLE,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name            : "load_tests_Python_Flink_Batch_GBK_1_${now}",
                        publish_to_big_query: false,
                        project             : 'apache-beam-testing',
                        metrics_dataset     : datasetName,
                        metrics_table       : "python_flink_batch_GBK_1",
                        input_options       : '\'{"num_records": 200000000,"key_size": 1,"value_size":9}\'',
                        iterations          : 1,
                        fanout              : 1,
                        parallelism         : parallelism,
                        job_endpoint: 'localhost:8099',
                        environment_config : pythonHarnessImageTag,
                        environment_type: 'DOCKER'

                ]
        ]}

def loadTest = { scope, triggeringContext ->
  scope.description('Runs Java GBK load tests on Flink runner in batch mode')
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

  def datasetName = loadTestsBuilder.getBigQueryDataset('load_test', triggeringContext)
  def testConfig = testConfiguration(datasetName)

  Flink.setUp(scope, 'beam_LoadTests_Python_GBK_Flink_Batch', CommonTestProperties.SDK.PYTHON, parallelism)

  loadTestsBuilder.loadTest(scope, testConfig.title, testConfig.runner, testConfig.sdk, testConfig.jobProperties, testConfig.itClass)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_LoadTests_Python_GBK_Flink_Batch',
        'Run Load Tests Python GBK Flink Batch',
        'Load Tests Python GBK Flink Batch suite',
        this
) {
  loadTest(delegate, CommonTestProperties.TriggeringContext.PR)
}

CronJobBuilder.cronJob('beam_LoadTests_Python_GBK_Flink_Batch', 'H 12 * * *', this) {
  loadTest(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}
