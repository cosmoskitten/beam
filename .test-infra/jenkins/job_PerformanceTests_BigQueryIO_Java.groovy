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
import PhraseTriggeringPostCommitBuilder

def now = new Date().format("MMddHHmmss", TimeZone.getTimeZone('UTC'))

def testJobs = [
        [
                title        : 'BigQueryIO Streaming Performance Test Java 10 GB',
                triggerPhrase: 'Run BigQueryIO Streaming Performance Test Java',
                jobName      : 'beam_BiqQueryIO_Streaming_Performance_Test_Java',
                itClass      : 'org.apache.beam.sdk.bigqueryioperftests.BigQueryIOIT',
                jobProperties: [
                        project               : 'apache-beam-testing',
                        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                        tempRoot              : 'gs://temp-storage-for-perf-tests/loadtests',
                        writeMethod           : 'STREAMING_INSERTS',
                        testBigQueryDataset   : 'beam_performance',
                        testBigQueryTable     : 'bqio_write_10GB_java',
                        metricsBigQueryDataset: 'beam_performance',
                        metricsBigQueryTable  : 'bqio_10GB_results_java_stream',
                        sourceOptions         : '{' +
                                '\\\'numRecords\\\': 10485760,' +
                                '\\\'keySizeBytes\\\': 1,' +
                                '\\\'valueSizeBytes\\\': 1024}',
                        runner                : 'DataflowRunner',
                        maxNumWorkers         : '5',
                        numWorkers            : '5',
                        autoscalingAlgorithm  : 'NONE',  // Disable autoscale the worker pool.
                ]
        ],
        [
                title        : 'BigQueryIO Batch Performance Test Java 10 GB',
                triggerPhrase: 'Run BigQueryIO Batch Performance Test Java',
                jobName      : 'beam_BiqQueryIO_Batch_Performance_Test_Java',
                itClass      : 'org.apache.beam.sdk.bigqueryioperftests.BigQueryIOIT',
                jobProperties: [
                        project               : 'apache-beam-testing',
                        tempLocation          : 'gs://temp-storage-for-perf-tests/loadtests',
                        tempRoot              : 'gs://temp-storage-for-perf-tests/loadtests',
                        writeMethod           : 'FILE_LOADS',
                        testBigQueryDataset   : 'beam_performance',
                        testBigQueryTable     : 'bqio_write_10GB_java',
                        metricsBigQueryDataset: 'beam_performance',
                        metricsBigQueryTable  : 'bqio_10GB_results_java_batch',
                        sourceOptions         : '{' +
                                '\\\'numRecords\\\': 10485760,' +
                                '\\\'keySizeBytes\\\': 1,' +
                                '\\\'valueSizeBytes\\\': 1024}',
                        runner                : "DataflowRunner",
                        maxNumWorkers         : '5',
                        numWorkers            : '5',
                        autoscalingAlgorithm  : 'NONE',  // Disable autoscale the worker pool.
                ]
        ]
]

testJobs.forEach { jobConfig -> createPostCommitJob(jobConfig)}

private void createPostCommitJob(testJob) {
    job(testJob.jobName) {
        description(testJob.description)
        common.setTopLevelMainJobProperties(delegate)
        common.enablePhraseTriggeringFromPullRequest(delegate, testJob.title, testJob.triggerPhrase)
        common.setAutoJob(delegate, 'H */6 * * *')

        String runner = "dataflow"
        String testTask = ':sdks:java:io:bigquery-io-perf-tests:integrationTest'

        steps {
            gradle {
                rootBuildScriptDir(common.checkoutDir)
                common.setGradleSwitches(delegate)
                switches("--info")
                switches("-DintegrationTestPipelineOptions=\'${common.joinPipelineOptions(testJob.jobProperties)}\'")
                switches("-DintegrationTestRunner=\'${runner}\'")
                tasks("${testTask} --tests ${testJob.itClass}")
            }
        }
    }
}