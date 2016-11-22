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

import common_job_properties

// Defines a job.
mavenJob('beam_PostCommit_Java_MavenInstall') {
  description('Runs postcommit tests on the Java SDK.')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelJobProperties(delegate)

  // Set maven paramaters.
  common_job_properties.setMavenConfig(delegate)

  // Set build triggers (7AM each day.)
  triggers {
    cron('0 7 * * *')
  }

  // Maven goals for this job.
  goals('-B -e clean deploy -P release,dataflow-runner -DskipITs=false -DintegrationTestPipelineOptions=\'[ "--project=apache-beam-testing", "--tempRoot=gs://temp-storage-for-end-to-end-tests", "--runner=org.apache.beam.runners.dataflow.testing.TestDataflowRunner" ]\'')

  publishers {
    // Notify the mailing list for each failed build.
    mailer('dev@beam.incubator.apache.org', false, false)
  }
}
