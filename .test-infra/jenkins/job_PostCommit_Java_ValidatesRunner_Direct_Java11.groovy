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
import PhraseTriggeringPostCommitBuilder


// This job runs the suite of ValidatesRunner tests against the Dataflow
// runner.
PhraseTriggeringPostCommitBuilder.postCommitJob('beam_PostCommit_Java_ValidatesRunner_Direct',
        'Run Direct ValidatesRunner in Java 11', 'Direct Runner ValidatesRunner Tests for Java 11', this) {

    description('Runs the ValidatesRunner suite on the Direct runner.')

    def JAVA_11_HOME = '/home/jenkins/tools/java/latest11'
    def JAVA_8_HOME = '/home/jenkins/tools/java/latest1.8'
    // Set common parameters. Sets a 3 hour timeout.
    commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 300)
    previousNames(/beam_PostCommit_Java_ValidatesRunner_Dataflow_Gradle/)

    // Publish all test results to Jenkins
    publishers {
        archiveJunit('**/build/test-results/**/*.xml')
    }

    // Gradle goals for this job.
    steps {
        gradle {
            rootBuildScriptDir(commonJobProperties.checkoutDir)
            tasks(':beam-runners-direct-java:shadowJar')
            tasks(':beam-runners-direct-java:shadowTestJar')
            switches("-Dorg.gradle.java.home=${JAVA_8_HOME}")
        }

        gradle {
            rootBuildScriptDir(commonJobProperties.checkoutDir)
            tasks(':beam-runners-direct-java:validatesRunner')
            switches("-Dorg.gradle.java.home=${JAVA_11_HOME}")
            switches('-x shadowJar')
            switches('-x shadowTestJar')
            switches('-x compileJava')
            switches('-x compileTestJava')
        }
    }

}
