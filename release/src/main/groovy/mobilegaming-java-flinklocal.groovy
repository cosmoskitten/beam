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
 * Run the mobile game examples on FlinkRunner.
 * https://beam.apache.org/get-started/mobile-gaming-example/
 */

t.describe 'Run Apache Beam Java SDK Mobile Gaming Examples - Flink'

QuickstartArchetype.generate(t)

t.intent 'Running the Mobile-Gaming Code with Flink'


/**
 * Run the UserScore example with Flink
 * */

t.intent("Running: UserScore example on Flink")
t.run(MobileGamingJavaUtils.generateCommand("UserScore", "FlinkRunner", t))
t.run "grep user19_BananaWallaby java-userscore-result-flink-runner.txt* "
t.see "total_score: 231, user: user19_BananaWallaby"
t.intent("SUCCEED: UserScore successfully run on FlinkRunner.")


/**
 * Run the HourlyTeamScore example with Flink
 * */

t.intent("Running: HourlyTeamScore example on Flink")
t.run(MobileGamingJavaUtils.generateCommand("HourlyTeamScore", "FlinkRunner", t))
t.run "grep AzureBilby java-hourlyteamscore-result-flink-runner.txt* "
t.see "total_score: 2788, team: AzureBilby"
t.intent("SUCCEED: HourlyTeamScore successfully run on FlinkRunner.")


/**
 * Run the LeaderBoard example with Flink
 * */

t.intent("Running: LeaderBoard example on Flink")
t.run("bq rm -f -t ${t.bqDataset()}.leaderboard_FlinkRunner_user")
t.run("bq rm -f -t ${t.bqDataset()}.leaderboard_FlinkRunner_team")

def InjectorThread = Thread.start() {
    t.run(MobileGamingJavaUtils.generateCommand("Injector", "",t))
}
def LeaderBoardThread = Thread.start() {
    t.run(MobileGamingJavaUtils.generateCommand("LeaderBoard", "FlinkRunner",t))
}
// wait 15 minutes for pipeline running
t.run("sleep 900")
InjectorThread.stop()
LeaderBoardThread.stop()

t.run "bq query SELECT table_id FROM ${t.bqDataset()}.__TABLES_SUMMARY__"
t.see "leaderboard_FlinkRunner_user"
t.see "leaderboard_FlinkRunner_team"
t.run """bq query --batch "SELECT user FROM [${t.gcpProject()}:${t.bqDataset()}.leaderboard_FlinkRunner_user] LIMIT 10\""""
t.seeOneOf(MobileGamingJavaUtils.COLORS)
t.intent("SUCCEED: LeaderBoard successfully run on FlinkRunner.")


/**
 * Run the GameStats example with Flink
 * */

t.intent("Running: GameStats example on FlinkRunner")
t.run("bq rm -f -t ${t.bqDataset()}.gamestats_FlinkRunner_sessions")
t.run("bq rm -f -t ${t.bqDataset()}.gamestats_FlinkRunner_team")
InjectorThread = Thread.start() {
    t.run(MobileGamingJavaUtils.generateCommand("Injector", "",t))
}

def GameStatsThread = Thread.start() {
    t.run(MobileGamingJavaUtils.generateCommand("GameStats", "FlinkRunner", t))
}

t.run("sleep 600")
InjectorThread.stop()
GameStatsThread.stop()
t.run("sleep 10")

t.run "bq query SELECT table_id FROM ${t.bqDataset()}.__TABLES_SUMMARY__"
t.see "gamestats_FlinkRunner_sessions"
t.see "gamestats_FlinkRunner_team"
t.run """bq query --batch "SELECT team FROM [${t.gcpProject()}:${t.bqDataset()}.gamestats_FlinkRunner_team] LIMIT 10\""""
t.seeOneOf(MobileGamingJavaUtils.COLORS)
t.intent("SUCCEED: GameStats successfully run on FlinkRunner.")

t.done()
