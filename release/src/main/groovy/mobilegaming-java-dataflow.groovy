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

t.intent 'Running the Mobile-Gaming Code with Dataflow runner'
StringBuilder cmd = new StringBuilder()
StringBuilder exeArgs = new StringBuilder()


/**
 *  Run the UserScore example on DataflowRunner
 * */

t.intent("Running: UserScore example on DataflowRunner")

t.run(MobileGamingJavaUtils.generateCommand("UserScore", "DataflowRunner", t))
t.run "gsutil cat gs://${t.gcsBucket()}/java-userscore-result-dataflow-runner* | grep user19_BananaWallaby"
t.see "total_score: 231, user: user19_BananaWallaby"
t.intent("SUCCEED: UserScore successfully run on DataflowRunner.")
t.run "gsutil rm gs://${t.gcsBucket()}/java-userscore-result-dataflow-runner*"


/**
 * Run the HourlyTeamScore example on DataflowRunner
 * */

t.intent("Running: HourlyTeamScore example on DataflowRunner")
t.run(MobileGamingJavaUtils.generateCommand("HourlyTeamScore", "DataflowRunner", t))
t.run "gsutil cat gs://${t.gcsBucket()}/java-hourlyteamscore-result-dataflow-runner* | grep AzureBilby "
t.see "total_score: 2788, team: AzureBilby"
t.intent("SUCCEED: HourlyTeamScore successfully run on DataflowRunner.")
t.run "gsutil rm gs://${t.gcsBucket()}/java-hourlyteamscore-result-dataflow-runner*"


/**
 * Run the LeaderBoard example on DataflowRunner
 * */

t.intent("Running: LeaderBoard example on DataflowRunner")
t.run("bq rm -f -t ${t.bqDataset()}.leaderboard_DataflowRunner_user")
t.run("bq rm -f -t ${t.bqDataset()}.leaderboard_DataflowRunner_team")

def InjectorThread = Thread.start() {
    t.run(MobileGamingJavaUtils.generateCommand("Injector", "",t))
}

def LeaderBoardThread = Thread.start() {
    t.run(MobileGamingJavaUtils.generateCommand("LeaderBoard", "DataflowRunner", t))
}
// wait 15 minutes for pipeline running
t.run("sleep 600")
InjectorThread.stop()
LeaderBoardThread.stop()

t.run "bq query SELECT table_id FROM ${t.bqDataset()}.__TABLES_SUMMARY__"
t.see "leaderboard_DataflowRunner_user"
t.see "leaderboard_DataflowRunner_team"
t.run """bq query --batch "SELECT user FROM [${t.gcpProject()}:${t.bqDataset()}.leaderboard_DataflowRunner_user] LIMIT 10\""""
t.seeOneOf(MobileGamingJavaUtils.COLORS)
t.intent("SUCCEED: LeaderBoard successfully run on DataflowRunner.")
exeArgs.setLength(0)
cmd.setLength(0)


/**
 *  Run the GameStats example on DataflowRunner
 * */

t.intent("Running: GameStats example on DataflowRunner")
t.run("bq rm -f -t ${t.bqDataset()}.gamestats_DataflowRunner_sessions")
t.run("bq rm -f -t ${t.bqDataset()}.gamestats_DataflowRunner_team")
InjectorThread = Thread.start() {
    t.run(MobileGamingJavaUtils.generateCommand("Injector", "",t))
}

def GameStatsThread = Thread.start() {
    t.run(MobileGamingJavaUtils.generateCommand("GameStats", "DataflowRunner", t))
}

t.run("sleep 600")
InjectorThread.stop()
GameStatsThread.stop()
t.run("sleep 10")

t.run "bq query SELECT table_id FROM ${t.bqDataset()}.__TABLES_SUMMARY__"
t.see "gamestats_DataflowRunner_sessions"
t.see "gamestats_DataflowRunner_team"
t.run """bq query --batch "SELECT team FROM [${t.gcpProject()}:${t.bqDataset()}.gamestats_DataflowRunner_team] LIMIT 10\""""
t.seeOneOf(MobileGamingJavaUtils.COLORS)
t.intent("SUCCEED: GameStats successfully run on DataflowRunner.")

t.done()
