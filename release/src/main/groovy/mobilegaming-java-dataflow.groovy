#!groovy
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

import MobileGamingJavaUtils

t = new TestScripts(args)

/*
 * Run the mobile game examples on Dataflow.
 * https://beam.apache.org/get-started/mobile-gaming-example/
 */

t.describe 'Run Apache Beam Java SDK Mobile Gaming Examples - Dataflow'

QuickstartArchetype.generate(t)

t.intent 'Running the Mobile-Gaming Code with DataflowRunner'

def runner = "DataflowRunner"

/**
 *  Run the UserScore example on DataflowRunner
 * */

t.intent("Running: UserScore example on DataflowRunner")
t.run(MobileGamingJavaUtils.createExampleExecutionCommand("UserScore", runner, t))
t.run "gsutil cat gs://${t.gcsBucket()}/${MobileGamingJavaUtils.getUserScoreOutputName(runner)}* | grep user19_BananaWallaby"
t.see "total_score: 231, user: user19_BananaWallaby"
t.intent("SUCCEED: UserScore successfully run on DataflowRunner.")
t.run "gsutil rm gs://${t.gcsBucket()}/${MobileGamingJavaUtils.getUserScoreOutputName(runner)}*"


/**
 * Run the HourlyTeamScore example on DataflowRunner
 * */

t.intent("Running: HourlyTeamScore example on DataflowRunner")
t.run(MobileGamingJavaUtils.createExampleExecutionCommand("HourlyTeamScore", runner, t))
t.run "gsutil cat gs://${t.gcsBucket()}/${MobileGamingJavaUtils.getHourlyTeamScoreOutputName(runner)}* | grep AzureBilby "
t.see "total_score: 2788, team: AzureBilby"
t.intent("SUCCEED: HourlyTeamScore successfully run on DataflowRunner.")
t.run "gsutil rm gs://${t.gcsBucket()}/${MobileGamingJavaUtils.getHourlyTeamScoreOutputName(runner)}*"


/**
 * Run the LeaderBoard example on DataflowRunner
 * */

t.intent("Running: LeaderBoard example on DataflowRunner")
t.run("bq rm -f -t ${t.bqDataset()}.leaderboard_DataflowRunner_user")
t.run("bq rm -f -t ${t.bqDataset()}.leaderboard_DataflowRunner_team")
// it will take couple seconds to clean up tables. This loop makes sure tables are completely deleted before running the pipeline
while({
  sleep(3000)
  t.run ("bq query SELECT table_id FROM ${t.bqDataset()}.__TABLES_SUMMARY__")
  t.seeExact("leaderboard_${runner}_user") || t.seeExact("leaderboard_${runner}_team")
}());

def InjectorThread = Thread.start() {
  t.run(MobileGamingJavaUtils.createInjectorCommand(t))
}

def LeaderBoardThread = Thread.start() {
  t.run(MobileGamingJavaUtils.createExampleExecutionCommand("LeaderBoard", runner, t))
}

// verify outputs in BQ tables
def startTime = System.currentTimeMillis()
def isSuccess = false
while((System.currentTimeMillis() - startTime)/60000 < MobileGamingJavaUtils.EXECUTION_TIMEOUT) {
  t.run "bq query SELECT table_id FROM ${t.bqDataset()}.__TABLES_SUMMARY__"
  if(t.seeExact("leaderboard_${runner}_user") && t.seeExact("leaderboard_${runner}_team")){
    t.run """bq query --batch "SELECT user FROM [${t.gcpProject()}:${t.bqDataset()}.leaderboard_${runner}_user] LIMIT 10\""""
    if(t.seeOneOf(MobileGamingJavaUtils.COLORS)){
      isSuccess = true
      break
    }
  }
  println "Waiting for verifying results..."
  sleep(60000) // wait for 1 min
}
InjectorThread.stop()
LeaderBoardThread.stop()

if(!isSuccess){
  t.error("FAILED: Failed running LeaderBoard on DataflowRunner")
}
t.intent("SUCCEED: LeaderBoard successfully run on DataflowRunner.")

t.done()
