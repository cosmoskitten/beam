/* * Licensed to the Apache Software Foundation (ASF) under one
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

job('beam_sonarqube_report_test') {
  commonJobProperties.setTopLevelMainJobProperties(
        delegate, 'master', 20,
        true) // needed for included regions PR triggering; see [JENKINS-23606]

  /**
   * https://issues.jenkins-ci.org/browse/JENKINS-42741
   */
  wrappers {
    withSonarQubeEnv {
      installationName('ASF Sonar Analysis')
    }
  }

  commonJobProperties.setPullRequestBuildTrigger(
    delegate,
    "test github pull request reporting",
    "run sq report test",
    true) // only trigger on phrase

  publishers {
    archiveJunit('**/build/test-results/**/*.xml')
  }


  steps {
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(":beam-runners-google-cloud-dataflow-java-fn-api-worker:test")
      tasks(":beam-runners-google-cloud-dataflow-java-fn-api-worker:sonarqube")
    }
  }
}
