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

// This job runs the suite of ValidatesRunner tests against the Direct runner.
job('beam_PostCommit_Java_Nexmark_Direct') {
  description('Runs the Nexmark suite on the Direct runner.')

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(delegate, 'master', 240)

  // Sets that this is a PostCommit job.
  common_job_properties.setPostCommit(delegate)

  // Allows triggering this build against pull requests.
  common_job_properties.enablePhraseTriggeringFromPullRequest(
    delegate,
    'Apache Direct Runner Nexmark Tests',
    'Run Direct Nexmark')

  // Gradle goals for this job.
  steps {
    shell('echo *** RUN NEXMARK IN BATCH MODE USING DIRECT RUNNER ***')
    gradle {
      rootBuildScriptDir(common_job_properties.checkoutDir)
      tasks(':beam-sdks-java-nexmark:run')
      common_job_properties.setGradleSwitches(delegate)
      switches('-Pnexmark.runner=":beam-runners-direct-java"' +
              ' -Pnexmark.args="' +
              '        --sinkType=BIGQUERY\n' +
              '        --runner=DirectRunner\n' +
              '        --streaming=false\n' +
              '        --suite=SMOKE\n' +
              '        --manageResources=false\n' +
              '        --monitorJobs=true\n' +
              '        --enforceEncodability=true\n' +
              '        --enforceImmutability=true"')
    }
    shell('echo *** RUN NEXMARK IN STREAMING MODE USING DIRECT RUNNER ***')
    gradle {
      rootBuildScriptDir(common_job_properties.checkoutDir)
      tasks(':beam-sdks-java-nexmark:run')
      common_job_properties.setGradleSwitches(delegate)
      switches('-Pnexmark.runner=":beam-runners-direct-java"' +
              ' -Pnexmark.args="' +
              '        --sinkType=BIGQUERY\n' +
              '        --runner=DirectRunner\n' +
              '        --streaming=true\n' +
              '        --suite=SMOKE\n' +
              '        --manageResources=false\n' +
              '        --monitorJobs=true\n' +
              '        --enforceEncodability=true\n' +
              '        --enforceImmutability=true"')
    }
  }
}
