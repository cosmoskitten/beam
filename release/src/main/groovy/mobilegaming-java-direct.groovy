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
 * Run the mobile game examples on DirectRunner.
 * https://beam.apache.org/get-started/mobile-gaming-example/
 */

t.describe 'Run Apache Beam Java SDK Mobile Gaming Examples - Direct'

QuickstartArchetype.generate(t)

t.intent 'Running the Mobile-Gaming Code with DirectRunner'

def runner = "DirectRunner"

/**
 * Run the UserScore example with DirectRunner
 * */

t.intent("Running: UserScore example on DirectRunner")
t.run(MobileGamingJavaUtils.generateCommand("UserScore", runner, t))
t.run "grep user19_BananaWallaby ${MobileGamingJavaUtils.getUserScoreOutputName(runner)}* "
t.see "total_score: 231, user: user19_BananaWallaby"
t.intent("SUCCEED: UserScore successfully run on DirectRunners.")


/**
 * Run the HourlyTeamScore example with DirectRunner
 * */

t.intent("Running: HourlyTeamScore example on DirectRunner")
t.run(MobileGamingJavaUtils.generateCommand("HourlyTeamScore", runner, t))
t.run "grep AzureBilby ${MobileGamingJavaUtils.getHourlyTeamScoreOutputName(runner)}* "
t.see "total_score: 2788, team: AzureBilby"
t.intent("SUCCEED: HourlyTeamScore successfully run on DirectRunners.")


/**
 * Run the LeaderBoard example with DirectRunner
 * */

t.intent("Running: LeaderBoard example on DirectRunner")
t.run("bq rm -f -t ${t.bqDataset()}.leaderboard_DirectRunner_user")
t.run("bq rm -f -t ${t.bqDataset()}.leaderboard_DirectRunner_team")
// it will take couple seconds to clean up tables. This loop makes sure tables are completely deleted before running the pipeline
while({
    sleep(3000)
    t.run ("bq query SELECT table_id FROM ${t.bqDataset()}.__TABLES_SUMMARY__")
    t.seeKeyWord("leaderboard_${runner}_user") || t.seeKeyWord("leaderboard_${runner}_team")
}());

def InjectorThread = Thread.start() {
    t.run(MobileGamingJavaUtils.generateCommand("Injector", "",t))
}

def LeaderBoardThread = Thread.start() {
    t.run(MobileGamingJavaUtils.generateCommand("LeaderBoard", runner, t))
}

// verify outputs in BQ tables
def startTime = System.currentTimeMillis()
def isSuccess = false
while((System.currentTimeMillis() - startTime)/60000 < MobileGamingJavaUtils.EXECUTION_TIMEOUT) {
    t.run "bq query SELECT table_id FROM ${t.bqDataset()}.__TABLES_SUMMARY__"
    if(t.seeKeyWord("leaderboard_${runner}_user") && t.seeKeyWord("leaderboard_${runner}_team")){
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
    t._error("FAILED: Failed running LeaderBoard on DirectRunner")
}
t.intent("SUCCEED: LeaderBoard successfully run on DirectRunner.")

t.done()
