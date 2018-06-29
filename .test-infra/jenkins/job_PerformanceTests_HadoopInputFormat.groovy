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

import CommonProperties as commonProperties

String jobName = "beam_PerformanceTests_HadoopInputFormat"

job(jobName) {
    // Set default Beam job properties.
    commonProperties.setTopLevelMainJobProperties(delegate)

    // Run job in postcommit every 6 hours, don't trigger every push, and
    // don't email individual committers.
    commonProperties.setAutoJob(
            delegate,
            'H */6 * * *')

    commonProperties.enablePhraseTriggeringFromPullRequest(
            delegate,
            'Java HadoopInputFormatIO Performance Test',
            'Run Java HadoopInputFormatIO Performance Test')

    def pipelineOptions = [
            tempRoot       : 'gs://temp-storage-for-perf-tests',
            project        : 'apache-beam-testing',
            postgresPort   : '5432',
            numberOfRecords: '600000'
    ]

    String namespace = commonProperties.getKubernetesNamespace(jobName)
    String kubeconfig = commonProperties.getKubeconfigLocationForNamespace(namespace)

    def testArgs = [
            kubeconfig              : kubeconfig,
            beam_it_timeout         : '1200',
            benchmarks              : 'beam_integration_benchmark',
            beam_prebuilt           : 'false',
            beam_sdk                : 'java',
            beam_it_module          : 'sdks/java/io/hadoop-input-format',
            beam_it_class           : 'org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIOIT',
            beam_it_options         : commonProperties.joinPipelineOptions(pipelineOptions),
            beam_kubernetes_scripts : commonProperties.makePathAbsolute('src/.test-infra/kubernetes/postgres/postgres-service-for-local-dev.yml'),
            beam_options_config_file: commonProperties.makePathAbsolute('src/.test-infra/kubernetes/postgres/pkb-config-local.yml'),
            bigquery_table          : 'beam_performance.hadoopinputformatioit_pkb_results'
    ]

    commonProperties.setupKubernetes(delegate, namespace, kubeconfig)
    commonProperties.buildPerformanceTest(delegate, testArgs)
    commonProperties.cleanupKubernetes(delegate, namespace, kubeconfig)
}

