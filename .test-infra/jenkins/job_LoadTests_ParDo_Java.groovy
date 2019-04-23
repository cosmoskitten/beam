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
import CronJobBuilder

def commonLoadTestConfig = { jobName, isStreaming ->
    [
            [
            title        : 'Load test: 2GB of 100B records 10 times',
            itClass      : 'org.apache.beam.sdk.loadtests.ParDoLoadTest',
            runner       : CommonTestProperties.Runner.DATAFLOW,
            jobProperties: [
                    project             : 'apache-beam-testing',
                    appName             : "load_tests_Java_Dataflow_${jobName}_ParDo_1",
                    tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
                    publishToBigQuery   : true,
                    bigQueryDataset     : 'load_test',
                    bigQueryTable       : "java_dataflow_${jobName}_ParDo_1",
                    sourceOptions       : """
                                            {
                                              "numRecords": 20000000,
                                              "keySizeBytes": 10,
                                              "valueSizeBytes": 90
                                            }
                                       """.trim().replaceAll("\\s", ""),
                    fanout              : 1,
                    iterations          : 10,
                    maxNumWorkers       : 5,
                    numWorkers          : 5,
                    stepOptions         : """
                                        {
                                            "outputRecordsPerInputRecord": 1,
                                            "preservesInputKeyDistribution": true,
                                            "reportThrottlingMicros": false
                                        }
                                        """.trim().replaceAll("\\s", ""),
                    autoscalingAlgorithm: "NONE",
                    streaming           : isStreaming,
                    numberOfCounterOperations: 0
            ]
            ],
            [
                    title        : 'Load test: 2GB of 100B records 200 times',
                    itClass      : 'org.apache.beam.sdk.loadtests.ParDoLoadTest',
                    runner       : CommonTestProperties.Runner.DATAFLOW,
                    jobProperties: [
                            project             : 'apache-beam-testing',
                            appName             : "load_tests_Java_Dataflow_${jobName}_ParDo_2",
                            tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery   : true,
                            bigQueryDataset     : 'load_test',
                            bigQueryTable       : "java_dataflow_${jobName}_ParDo_2",
                            sourceOptions       : """
                                                    {
                                                      "numRecords": 20000000,
                                                      "keySizeBytes": 10,
                                                      "valueSizeBytes": 90
                                                    }
                                               """.trim().replaceAll("\\s", ""),
                            fanout              : 1,
                            iterations          : 200,
                            maxNumWorkers       : 5,
                            numWorkers          : 5,
                            stepOptions         : """
                                        {
                                            "outputRecordsPerInputRecord": 1,
                                            "preservesInputKeyDistribution": true,
                                            "reportThrottlingMicros": false
                                        }
                                        """.trim().replaceAll("\\s", ""),
                            autoscalingAlgorithm: "NONE",
                            streaming           : isStreaming,
                            numberOfCounterOperations: 0
                    ]
            ],
            [

                    title        : 'Load test: 2GB of 100B records 10 counters',
                    itClass      : 'org.apache.beam.sdk.loadtests.ParDoLoadTest',
                    runner       : CommonTestProperties.Runner.DATAFLOW,
                    jobProperties: [
                            project             : 'apache-beam-testing',
                            appName             : "load_tests_Java_Dataflow_${jobName}_ParDo_3",
                            tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery   : true,
                            bigQueryDataset     : 'load_test',
                            bigQueryTable       : "java_dataflow_${jobName}_ParDo_3",
                            sourceOptions       : """
                                                    {
                                                      "numRecords": 20000000,
                                                      "keySizeBytes": 10,
                                                      "valueSizeBytes": 90
                                                    }
                                               """.trim().replaceAll("\\s", ""),
                            fanout              : 1,
                            iterations          : 10,
                            maxNumWorkers       : 5,
                            numWorkers          : 5,
                            stepOptions         : """
                                        {
                                            "outputRecordsPerInputRecord": 1,
                                            "preservesInputKeyDistribution": true,
                                            "reportThrottlingMicros": false
                                        }
                                        """.trim().replaceAll("\\s", ""),
                            autoscalingAlgorithm: "NONE",
                            streaming           : isStreaming,
                            numberOfCounterOperations: 10
                    ]

            ],
            [
                    title        : 'Load test: 2GB of 100B records 100 counters',
                    itClass      : 'org.apache.beam.sdk.loadtests.ParDoLoadTest',
                    runner       : CommonTestProperties.Runner.DATAFLOW,
                    jobProperties: [
                            project             : 'apache-beam-testing',
                            appName             : "load_tests_Java_Dataflow_${jobName}_ParDo_4",
                            tempLocation        : 'gs://temp-storage-for-perf-tests/loadtests',
                            publishToBigQuery   : true,
                            bigQueryDataset     : 'load_test',
                            bigQueryTable       : "java_dataflow_${jobName}_ParDo_4",
                            sourceOptions       : """
                                                    {
                                                      "numRecords": 20000000,
                                                      "keySizeBytes": 10,
                                                      "valueSizeBytes": 90
                                                    }
                                               """.trim().replaceAll("\\s", ""),
                            fanout              : 1,
                            iterations          : 10,
                            maxNumWorkers       : 5,
                            numWorkers          : 5,
                            stepOptions         : """
                                        {
                                            "outputRecordsPerInputRecord": 1,
                                            "preservesInputKeyDistribution": true,
                                            "reportThrottlingMicros": false
                                        }
                                        """.trim().replaceAll("\\s", ""),
                            autoscalingAlgorithm: "NONE",
                            streaming           : isStreaming,
                            numberOfCounterOperations: 100
                    ]
            ]
    ]
}


def batchLoadTestJob = { scope, triggeringContext ->
    scope.description('Runs Java ParDo load tests on Dataflow runner in batch mode')
    commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

    for (testConfiguration in commonLoadTestConfig('batch', false)) {
        loadTestsBuilder.loadTest(scope, testConfiguration.title, testConfiguration.runner, CommonTestProperties.SDK.JAVA, testConfiguration.jobProperties, testConfiguration.itClass, triggeringContext)
    }
}

def streamingLoadTestJob = {scope, triggeringContext ->
    scope.description('Runs Java ParDo load tests on Dataflow runner in streaming mode')
    commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

    for (testConfiguration in commonLoadTestConfig('combine', true)) {
        testConfiguration.jobProperties << [inputWindowDurationSec: 1200]
        loadTestsBuilder.loadTest(scope, testConfiguration.title, testConfiguration.runner, CommonTestProperties.SDK.JAVA, testConfiguration.jobProperties, testConfiguration.itClass, triggeringContext)
    }
}

CronJobBuilder.cronJob('beam_LoadTests_Java_ParDo_Dataflow_Batch', 'H 12 * * *', this) {
    batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

CronJobBuilder.cronJob('beam_LoadTests_Java_ParDo_Dataflow_Streaming', 'H 12 * * *', this) {
    streamingLoadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_LoadTests_Java_ParDo_Dataflow_Batch',
        'Run Load Tests Java ParDo Dataflow Batch',
        'Load Tests Java ParDo Dataflow Batch suite',
        this
) {
    batchLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_LoadTests_Java_ParDo_Dataflow_Streaming',
        'Run Load Tests Java ParDo Dataflow Streaming',
        'Load Tests Java ParDo Dataflow Streaming suite',
        this
) {
    streamingLoadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
}