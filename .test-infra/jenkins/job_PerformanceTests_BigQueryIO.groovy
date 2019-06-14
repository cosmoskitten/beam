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

def testsConfigurations = [
        [
                jobName           : 'beam_PerformanceTests_BQIO_Read',
                jobDescription    : 'Runs BigQueryIO Read Performance Test',
                itClass           : 'apache_beam.io.gcp.bigquery_read_perf_test:BigQueryReadPerfTest.test',
                itModule          : 'sdks/python',
                bqTable           : 'beam_performance.bqio_read_10GB_results',
                prCommitStatusName: 'BigQueryIO Read Performance Test',
                prTriggerPhase    : 'Run BigQueryIO Read Performance Test',
                extraPipelineArgs: [
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
        ],
        [
                jobName            : 'beam_PerformanceTests_BQIO_Write',
                jobDescription     : 'Runs BigQueryIO Write Performance Test',
                itClass            : 'apache_beam.io.gcp.bigquery_write_perf_test:BigQueryWritePerfTest.test',
                itModule           : 'sdks/python',
                bqTable            : 'beam_performance.bqio_write_10GB_results',
                prCommitStatusName : 'BigQueryIO Write Performance Test',
                prTriggerPhase     : 'Run BigQueryIO Write Performance Test',
                extraPipelineArgs: [
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
        ],
]

for (testConfiguration in testsConfigurations) {
    create_bigqueryio_performance_test_job(testConfiguration)
}


private void create_bigqueryio_performance_test_job(testConfiguration) {

    // This job runs the file-based IOs performance tests on PerfKit Benchmarker.
    job(testConfiguration.jobName) {
        description(testConfiguration.jobDescription)

        // Set default Beam job properties.
        commonJobProperties.setTopLevelMainJobProperties(delegate)

        // Allows triggering this build against pull requests.
        commonJobProperties.enablePhraseTriggeringFromPullRequest(
                delegate,
                testConfiguration.prCommitStatusName,
                testConfiguration.prTriggerPhase)

        // Run job in postcommit every 6 hours, don't trigger every push, and
        // don't email individual committers.
        commonJobProperties.setAutoJob(
                delegate,
                'H */6 * * *')

        def pipelineOptions = [
                project         : 'apache-beam-testing',
                staging_location: 'gs://temp-storage-for-end-to-end-tests/staging-it',
                temp_location   : 'gs://temp-storage-for-end-to-end-tests/temp-it',
        ]
        if (testConfiguration.containsKey('extraPipelineArgs')) {
            pipelineOptions << testConfiguration.extraPipelineArgs
        }

        def argMap = [
                beam_sdk                : 'python',
                benchmarks              : 'beam_integration_benchmark',
                bigquery_table          : testConfiguration.bqTable,
                beam_it_class           : testConfiguration.itClass,
                beam_it_module          : testConfiguration.itModule,
                beam_prebuilt           : 'false',
                beam_python_sdk_location: commonJobProperties.getPythonSDKLocationFromModule('', testConfiguration.itModule),
                beam_runner             : 'TestDataflowRunner',
                beam_it_timeout         : '1200',
                beam_it_options         : commonJobProperties.joinPipelineOptions(pipelineOptions),
        ]
        commonJobProperties.buildPerformanceTest(delegate, argMap)
    }
}
