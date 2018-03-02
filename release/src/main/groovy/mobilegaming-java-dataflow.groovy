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

import MobileGamingTeamList
import MobileGamingJavaUtils

t = new TestScripts(args)

/*
 * Run the mobile game examples on DirectRunner.
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
//exeArgs.append("--project=${t.gcpProject()} ")
//        .append("--tempLocation=gs://${t.gcsBucket()}/tmp ")
//        .append("--runner=DataflowRunner ")
//        .append("--output=gs://${t.gcsBucket()}/java-userscore-result_dataflow.txt ")
//        .append("--input=gs://${t.gcsBucket()}/5000_gaming_data.csv")
//cmd.append("mvn compile exec:java -q ")
//        .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.UserScore ")
//        .append("-Dexec.args=\"${exeArgs.toString()}\" ")
//        .append("-Pdataflow-runner")
//t.run(cmd.toString())
t.run(MobileGamingJavaUtils.generateCommand("UserScore", "DataflowRunner", t))
t.run "gsutil cat gs://${t.gcsBucket()}/java-userscore-result-dataflow-runner* | grep user19_BananaWallaby"
t.see "total_score: 231, user: user19_BananaWallaby"
t.intent("SUCCEED: UserScore successfully run on DataflowRunner.")
t.run "gsutil rm gs://${t.gcsBucket()}/java-userscore-result-dataflow-runner*"
//exeArgs.setLength(0)
//cmd.setLength(0)


/**
 * Run the HourlyTeamScore example on DataflowRunner
 * */

t.intent("Running: HourlyTeamScore example on DataflowRunner")
//exeArgs.append("--project=${t.gcpProject()} ")
//        .append("--tempLocation=gs://${t.gcsBucket()}/tmp ")
//        .append("--runner=DataflowRunner ")
//        .append("--output=gs://${t.gcsBucket()}/java-hourlyteamscore-result_dataflow.txt ")
//        .append("--input=gs://${t.gcsBucket()}/5000_gaming_data.csv")
//cmd.append("mvn compile exec:java -q ")
//        .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.HourlyTeamScore ")
//        .append("-Dexec.args=\"${exeArgs.toString()}\" ")
//        .append("-Pdataflow-runner")
//t.run(cmd.toString())
t.run(MobileGamingJavaUtils.generateCommand("HourlyTeamScore", "DataflowRunner", t))
t.run "gsutil cat gs://${t.gcsBucket()}/java-hourlyteamscore-result-dataflow-runner* | grep AzureBilby "
t.see "total_score: 2788, team: AzureBilby"
t.intent("SUCCEED: HourlyTeamScore successfully run on DataflowRunner.")
t.run "gsutil rm gs://${t.gcsBucket()}/java-hourlyteamscore-result-dataflow-runner*"
//exeArgs.setLength(0)
//cmd.setLength(0)


/**
 * Run the LeaderBoard example on DataflowRunner
 * */

//t.intent("Running: LeaderBoard example on DataflowRunner")
//t.run("bq rm -f -t ${t.bqDataset()}.leaderboard_dataflow_user")
//t.run("bq rm -f -t ${t.bqDataset()}.leaderboard_dataflow_team")
//StringBuilder injectorCmd = new StringBuilder()
//injectorCmd.append("mvn compile exec:java ")
//        .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.injector.Injector ")
//        .append("-Dexec.args=\"${t.gcpProject()} ${t.pubsubTopic()} none\"")
//
//def InjectorThread = Thread.start() {
//    t.run injectorCmd.toString()
//}
//exeArgs.append("--project=${t.gcpProject()} ")
//        .append("--tempLocation=gs://${t.gcsBucket()}/tmp ")
//        .append("--runner=DataflowRunner ")
//        .append("--dataset=${t.bqDataset()} ")
//        .append("--topic=projects/${t.gcpProject()}/topics/${t.pubsubTopic()} ")
//        .append("--output=gs://${t.gcsBucket()}/java-leaderboard-dataflow-result.txt ")
//        .append("--leaderBoardTableName=leaderboard_dataflow")
//cmd.append("mvn compile exec:java -q ")
//        .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.LeaderBoard ")
//        .append("-Dexec.args=\"${exeArgs.toString()}\" ")
//        .append("-Pdataflow-runner")
//
//def LeaderBoardThread = Thread.start() {
//    t.run(cmd.toString())
//}
//// wait 15 minutes for pipeline running
//t.run("sleep 900")
//InjectorThread.stop()
//LeaderBoardThread.stop()
//
//t.run "bq query SELECT table_id FROM ${t.bqDataset()}.__TABLES_SUMMARY__"
//t.see "leaderboard_dataflow_user"
//t.see "leaderboard_dataflow_team"
//t.run """bq query --batch "SELECT user FROM [${t.gcpProject()}:${t.bqDataset()}.leaderboard_dataflow_user] LIMIT 10\""""
//t.seeOneOf(MobileGamingTeamList.COLORS)
//t.intent("SUCCEED: LeaderBoard successfully run on DataflowRunner.")
//exeArgs.setLength(0)
//cmd.setLength(0)
//
//
///**
// *  Run the GameStats example on DataflowRunner
// * */
//
//t.intent("Running: GameStats example on DataflowRunner")
//t.run("bq rm -f -t ${t.bqDataset()}.gamestats_dataflow_sessions")
//t.run("bq rm -f -t ${t.bqDataset()}.gamestats_dataflow_team")
//InjectorThread = Thread.start() {
//    t.run injectorCmd.toString()
//}
//
//exeArgs.append("--project=${t.gcpProject()} ")
//        .append("--tempLocation=gs://${t.gcsBucket()}/tmp ")
//        .append("--runner=DataflowRunner ")
//        .append("--dataset=${t.bqDataset()} ")
//        .append("--topic=projects/${t.gcpProject()}/topics/${t.pubsubTopic()} ")
//        .append("--output=gs://${t.gcsBucket()}/java-leaderboard-dataflow-result.txt ")
//        .append("--fixedWindowDuration=5 ")
//        .append("--userActivityWindowDuration=5 ")
//        .append("--sessionGap=1 ")
//        .append("--gameStatsTablePrefix=gamestats_dataflow")
//cmd.append("mvn compile exec:java -q ")
//        .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.GameStats ")
//        .append("-Dexec.args=\"${exeArgs.toString()}\" ")
//        .append("-Pdataflow-runner")
//
//def GameStatsThread = Thread.start() {
//    t.run(cmd.toString())
//}
//
//t.run("sleep 600")
//InjectorThread.stop()
//GameStatsThread.stop()
//t.run("sleep 10")
//
//t.run "bq query SELECT table_id FROM ${t.bqDataset()}.__TABLES_SUMMARY__"
//t.see "gamestats_dataflow_sessions"
//t.see "gamestats_dataflow_team"
//t.run """bq query --batch "SELECT team FROM [${t.gcpProject()}:${t.bqDataset()}.gamestats_dataflow_team] LIMIT 10\""""
//t.seeOneOf(MobileGamingTeamList.COLORS)
//t.intent("SUCCEED: GameStats successfully run on DataflowRunner.")
//exeArgs.setLength(0)
//cmd.setLength(0)

t.done()