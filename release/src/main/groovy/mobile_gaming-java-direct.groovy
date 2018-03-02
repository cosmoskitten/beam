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
GCS_BUCKET_SUBDIR = "mobile-gaming"
TEMP_DIR = "tmp"

t = new TestScripts(args)

/*
 * Run the mobile game examples on DirectRunner.
 * https://beam.apache.org/get-started/mobile-gaming-example/
 */

t.describe 'Run Apache Beam Java SDK Mobile Gaming Examples - Direct'

t.intent 'Gets the Mobile-Gaming Example Code'
QuickstartArchetype.generate(t)
//t.run "cd .."
//t.run "cd java-mobile-gaming-temp/word-count-beam"

t.intent 'Runs the Mobile-Gaming Code with Direct runner'
StringBuilder cmd = new StringBuilder()

// Run the UserScore example
t.intent("Running HourlyTeamScore example on DirectRunner")
cmd.append("mvn compile exec:java ")
.append("-Dexec.mainClass=org.apache.beam.examples.complete.game.UserScore ")
.append("""-Dexec.args="--tempLocation=gs://${t.gcsBucket()}/${t.gcsBucketSubDirectory}/tmp
             --output=java-userscore-result.txt
             --input=gs://${t.gcsBucket()}/${t.gcsBucketSubDirectory}/5000_gaming_data.csv\" """)
.append("-Pdirect-runner")
t.run(cmd.toString())

t.run "grep user19_BananaWallaby java-userscore-result.txt* "
t.see "total_score: 231, user: user19_BananaWallaby"
t.intent("SUCCEED: UserScore successfully run on DirectRunners.")
cmd.setLength(0)


// Run the HourlyTeamScore example
t.intent("Running HourlyTeamScore example on DirectRunner")
//t.run "gsutil rm gs://${t.gcsBucket()}/${t.gcsBucketSubDirectory}/java-hourlyteamscore-result.txt*"
cmd.append("mvn compile exec:java ")
.append("-Dexec.mainClass=org.apache.beam.examples.complete.game.HourlyTeamScore ")
.append("""-Dexec.args="--tempLocation=gs://${t.gcsBucket()}/${t.gcsBucketSubDirectory}/tmp
             --output=java-hourlyteamscore-result.txt
             --input=gs://${t.gcsBucket()}/${t.gcsBucketSubDirectory}/5000_gaming_data.csv\" """)
.append("-Pdirect-runner")
t.run(cmd.toString())
t.run "grep AzureBilby java-hourlyteamscore-result.txt* "
t.see "total_score: 2788, team: AzureBilby"
t.intent("SUCCEED: HourlyTeamScore successfully run on DirectRunners.")
cmd.setLength(0)


// Run LeaderBoard example
//StringBuilder injectorCmd = new StringBuilder()
//injectorCmd.append("mvn compile exec:java ")
//.append("-Dexec.mainClass=org.apache.beam.examples.complete.game.injector.Injector ")
//.append("-Dexec.args=\"google.com:dataflow-streaming python-leaderboard-topic none\"")
//
//def LeaderBoardThread = Thread.start() {
//    t.run injectorCmd.toString()
//}
//t.run("sleep 20")
//
//cmd.append("mvn compile exec:java ")
//.append("-Dexec.mainClass=org.apache.beam.examples.complete.game.LeaderBoard ")
//.append("""-Dexec.args=\"--project=google.com:dataflow-streaming
//             --tempLocation=gs://python-streaming-test/temp
//             --dataset=python_leader_board_test
//             --topic=projects/google.com:dataflow-streaming/topics/python-leaderboard-topic
//             --output=gs://python-streaming-test/java-leaderboard-result.txt\"""")
//t.run(cmd.toString())
//t.run("sleep 800")
//
//LeaderBoardThread.stop()

//t.run "bq query SELECT table_id FROM python_leader_board_test.__TABLES_SUMMARY__"
//t.see "leaderboard_user"
//t.see "leaderboard_team"

// t.run """bq query --batch "SELECT * FROM [google.com:dataflow-streaming:python_leader_board_test.leaderboard_user] LIMIT 1000\""""
// t.seeOneOf ***




// Run GameStats example


// Clean up
t.done()
