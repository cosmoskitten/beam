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

def bqio_read_test = [
        title        : 'BigQueryIO Read Performance Test Python 10 GB',
        itClass      : 'apache_beam.io.gcp.bigquery_read_perf_test:BigQueryReadPerfTest.test',
        runner       : CommonTestProperties.Runner.DATAFLOW,
        sdk          : CommonTestProperties.SDK.PYTHON,
        jobProperties: [
                job_name             : 'performance-tests-bqio-read-python-10gb' + now,
                project              : 'apache-beam-testing',
                temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
                input_dataset        : 'beam_performance',
                input_table          : 'bqio_read_10GB',
                publish_to_big_query : true,
                metrics_dataset      : 'beam_performance',
                metrics_table        : 'bqio_read_10GB_results',
                input_options        : '\'{"num_records": 10485760,' +
                        '"key_size": 1,' +
                        '"value_size": 1024}\'',
                autoscaling_algorithm: 'NONE',  // Disable autoscale the worker pool.
        ]
]

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_BiqQueryIO_Read_Performance_Test_Python',
        'Run BigQueryIO Read Performance Test Python',
        'BigQueryIO Read Performance Test Python',
        this
) {
        def testConfig = bqio_read_test
        loadTestsBuilder.loadTest(delegate, testConfig.title, testConfig.runner, testConfig.sdk, testConfig.jobProperties, testConfig.itClass, CommonTestProperties.TriggeringContext.PR)
}

CronJobBuilder.cronJob('beam_BiqQueryIO_Read_Performance_Test_Python', 'H 15 * * *', this) {
        def testConfig = bqio_read_test
        loadTestsBuilder.loadTest(delegate, testConfig.title, testConfig.runner, testConfig.sdk, testConfig.jobProperties, testConfig.itClass, CommonTestProperties.TriggeringContext.POST_COMMIT)
}


def bqio_write_test = [
        title        : 'BigQueryIO Write Performance Test Python Batch 10 GB',
        itClass      : 'apache_beam.io.gcp.bigquery_write_perf_test:BigQueryWritePerfTest.test',
        runner       : CommonTestProperties.Runner.DATAFLOW,
        sdk          : CommonTestProperties.SDK.PYTHON,
        jobProperties: [
                job_name             : 'performance-tests-bqio-write-python-batch-10gb' + now,
                project              : 'apache-beam-testing',
                temp_location        : 'gs://temp-storage-for-perf-tests/loadtests',
                output_dataset       : 'beam_performance',
                output_table         : 'bqio_write_10GB',
                publish_to_big_query : true,
                metrics_dataset      : 'beam_performance',
                metrics_table        : 'bqio_write_10GB_results',
                input_options        : '\'{"num_records": 10485760,' +
                        '"key_size": 1,' +
                        '"value_size": 1024}\'',
                autoscaling_algorithm: 'NONE',  // Disable autoscale the worker pool.
        ]
]

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_BiqQueryIO_Write_Performance_Test_Python_Batch',
        'Run BigQueryIO Write Performance Test Python Batch',
        'BigQueryIO Write Performance Test Python Batch',
        this
) {
        def testConfig = bqio_write_test
        loadTestsBuilder.loadTest(delegate, testConfig.title, testConfig.runner, testConfig.sdk, testConfig.jobProperties, testConfig.itClass, CommonTestProperties.TriggeringContext.PR)
}

CronJobBuilder.cronJob('beam_BiqQueryIO_Write_Performance_Test_Python_Batch', 'H 15 * * *', this) {
        def testConfig = bqio_write_test
        loadTestsBuilder.loadTest(delegate, testConfig.title, testConfig.runner, testConfig.sdk, testConfig.jobProperties, testConfig.itClass, CommonTestProperties.TriggeringContext.POST_COMMIT)
}
