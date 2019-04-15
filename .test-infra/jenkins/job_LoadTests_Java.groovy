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
import LoadTestsBuilder as loadTestsBuilder
import PhraseTriggeringPostCommitBuilder
import CronJobBuilder
import CommonLoadTestConfiguration as commonLoadTestConfig


def loadTestJob = { scope, triggeringContext ->
  scope.description('Runs Java GBK load tests on Dataflow runner in batch mode')
  commonJobProperties.setTopLevelMainJobProperties(scope, 'master', 240)

  for (testConfiguration in commonLoadTestConfig.loadTestConfigurations('batch', false)) {
    loadTestsBuilder.loadTest(scope, testConfiguration.title, testConfiguration.runner, CommonTestProperties.SDK.JAVA, testConfiguration.jobProperties, testConfiguration.itClass, triggeringContext)
  }
}

CronJobBuilder.cronJob('beam_LoadTests_Java_GBK_Dataflow_Batch', 'H 12 * * *', this) {
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.POST_COMMIT)
}

PhraseTriggeringPostCommitBuilder.postCommitJob(
        'beam_LoadTests_Java_GBK_Dataflow_Batch',
        'Run Load Tests Java GBK Dataflow Batch',
        'Load Tests Java GBK Dataflow Batch suite',
        this
) {
  loadTestJob(delegate, CommonTestProperties.TriggeringContext.PR)
}



