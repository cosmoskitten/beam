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

// This job runs the Java multi-JDK tests in postcommit, including WordCountIT.
matrixJob('beam_PostCommit_Java_Version_Test') {
  description('Runs postcommit tests on the Java SDK in multiple Jdk versions.')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(delegate)

  // Override jdk version here
  axes {
    label('label', 'beam')
    jdk('JDK 1.8 (latest)',
        'JDK 1.7 (latest)',
        'OpenJDK 7 (on Ubuntu only)',
        'OpenJDK 8 (on Ubuntu only)')
  }

  // Sets that this is a PostCommit job.
  common_job_properties.setPostCommit(delegate)

  // Allows triggering this build against pull requests.
  common_job_properties.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Java JDK Versions Test',
      'Run Java Versions Test')

  // Maven build for this job.
  steps {
    maven {
      // Set maven parameters.
//      common_job_properties.setMavenConfig(delegate)

      mavenInstallation('Maven 3.3.3')
      mavenOpts('-Dorg.slf4j.simpleLogger.showDateTime=true')
      mavenOpts('-Dorg.slf4j.simpleLogger.dateTimeFormat=yyyy-MM-dd\\\'T\\\'HH:mm:ss.SSS')
      // The -XX:+TieredCompilation -XX:TieredStopAtLevel=1 JVM options enable
      // tiered compilation to make the JVM startup times faster during the tests.
      mavenOpts('-XX:+TieredCompilation')
      mavenOpts('-XX:TieredStopAtLevel=1')
      rootPOM('pom.xml')
      // Use a repository local to the workspace for better isolation of jobs.
      localRepository(LocalRepositoryLocation.LOCAL_TO_WORKSPACE)


      
      // Maven build project
      goals('-B -e -P release,dataflow-runner clean install coveralls:report -DrepoToken=$COVERALLS_REPO_TOKEN -DskipITs=false -DintegrationTestPipelineOptions=\'[ "--project=apache-beam-testing", "--tempRoot=gs://temp-storage-for-end-to-end-tests", "--runner=org.apache.beam.runners.dataflow.testing.TestDataflowRunner" ]\'')
    }
  }
}
