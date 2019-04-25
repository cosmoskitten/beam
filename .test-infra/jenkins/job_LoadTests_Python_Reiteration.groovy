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

import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))


def loadTestWithReiterateConfigurations = [
        [
                title        : 'GroupByKey Python Load test: reiterate 4 times 10kB values',
                itClass      : 'apache_beam.testing.load_tests.group_by_key_test:GroupByKeyTest.testGroupByKey',
                runner       : CommonTestProperties.Runner.DATAFLOW,
                sdk          : CommonTestProperties.SDK.PYTHON,
                jobProperties: [
                        job_name             : 'load-tests-python-dataflow-batch-gbk-6-' + now,
                        project              : 'apache-beam-testing',
                        temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
                        publish_to_big_query : true,
                        metrics_dataset      : 'load_test',
                        metrics_table        : 'python_dataflow_batch_gbk_6',
                        input_options        : '\'{"num_records": 20000000,' +
                                '"key_size": 10,' +
                                '"value_size": 90}\'',
                        iterations           : 4,
                        fanout               : 1,
                        max_num_workers      : 5,
                        num_workers          : 5,
                        autoscaling_algorithm: "NONE"
                ]
        ]
]


PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_Python_LoadTests_GBK_Dataflow_Batch_With_Reiteration',
        'Run Python Load Tests GBK Dataflow Batch With Reiteration',
        'Load Tests Python GBK Dataflow Batch suite with reiteration',
        this
) {
        loadTestsBuilder.loadTests(delegate, CommonTestProperties.SDK.PYTHON, loadTestWithReiterateConfigurations, CommonTestProperties.TriggeringContext.PR, "GBK", "batch")
}

CronJobBuilder.cronJob('beam_LoadTests_Python_GBK_Dataflow_With_Reiteration_Batch', 'H 12 * * *', this) {
        loadTestsBuilder.loadTests(delegate, CommonTestProperties.SDK.PYTHON, loadTestConfigurations, CommonTestProperties.TriggeringContext.POST_COMMIT, "GBK", "batch")
}
