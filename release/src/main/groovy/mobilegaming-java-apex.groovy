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
 * Run the mobile game examples on Apex.
 * https://beam.apache.org/get-started/mobile-gaming-example/
 */

t.describe 'Run Apache Beam Java SDK Mobile Gaming Examples - Apex'

QuickstartArchetype.generate(t)

t.intent 'Running the Mobile-Gaming Code with Apex'


/**
 * Run the UserScore example with Apex
 * */

t.intent("Running: UserScore example on Apex")
t.run(MobileGamingJavaUtils.generateCommand("UserScore", "ApexRunner", t))
t.run "grep user19_BananaWallaby java-userscore-result-apex-runner.txt* "
t.see "total_score: 231, user: user19_BananaWallaby"
t.intent("SUCCEED: UserScore successfully run on ApexRunner.")


/**
 * Run the HourlyTeamScore example with Apex
 * */

t.intent("Running: HourlyTeamScore example on Apex")
t.run(MobileGamingJavaUtils.generateCommand("HourlyTeamScore", "ApexRunner", t))
t.run "grep AzureBilby java-hourlyteamscore-result-apex-runner.txt* "
t.see "total_score: 2788, team: AzureBilby"
t.intent("SUCCEED: HourlyTeamScore successfully run on ApexRunner.")


/**
 * Run the LeaderBoard example with Apex
 * */

t.intent("Running: LeaderBoard example on Apex")
t.run("bq rm -f -t ${t.bqDataset()}.leaderboard_ApexRunner_user")
t.run("bq rm -f -t ${t.bqDataset()}.leaderboard_ApexRunner_team")

def InjectorThread = Thread.start() {
    t.run(MobileGamingJavaUtils.generateCommand("Injector", "",t))
}
def LeaderBoardThread = Thread.start() {
    t.run(MobileGamingJavaUtils.generateCommand("LeaderBoard", "ApexRunner",t))
}
// wait 15 minutes for pipeline running
t.run("sleep 900")
InjectorThread.stop()
LeaderBoardThread.stop()

t.run "bq query SELECT table_id FROM ${t.bqDataset()}.__TABLES_SUMMARY__"
t.see "leaderboard_ApexRunner_user"
t.see "leaderboard_ApexRunner_team"
t.run """bq query --batch "SELECT user FROM [${t.gcpProject()}:${t.bqDataset()}.leaderboard_ApexRunner_user] LIMIT 10\""""
t.seeOneOf(MobileGamingJavaUtils.COLORS)
t.intent("SUCCEED: LeaderBoard successfully run on Apex.")


/**
 * Run the GameStats example with Apex
 * */

t.intent("Running: GameStats example on Apex")
t.run("bq rm -f -t ${t.bqDataset()}.gamestats_ApexRunner_sessions")
t.run("bq rm -f -t ${t.bqDataset()}.gamestats_ApexRunner_team")
InjectorThread = Thread.start() {
    t.run(MobileGamingJavaUtils.generateCommand("Injector", "",t))
}

def GameStatsThread = Thread.start() {
    t.run(MobileGamingJavaUtils.generateCommand("GameStats", "ApexRunner", t))
}

t.run("sleep 600")
InjectorThread.stop()
GameStatsThread.stop()
t.run("sleep 10")

t.run "bq query SELECT table_id FROM ${t.bqDataset()}.__TABLES_SUMMARY__"
t.see "gamestats_ApexRunner_sessions"
t.see "gamestats_ApexRunner_team"
t.run """bq query --batch "SELECT team FROM [${t.gcpProject()}:${t.bqDataset()}.gamestats_ApexRunner_team] LIMIT 10\""""
t.seeOneOf(MobileGamingJavaUtils.COLORS)
t.intent("SUCCEED: GameStats successfully run on ApexRunner.")

t.done()
