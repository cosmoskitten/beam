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

//t = new TestScripts(args)
t = new TestScripts()
/*
 * Run the mobile game examples on DirectRunner.
 * https://beam.apache.org/get-started/mobile-gaming-example/
 */

t.describe 'Run Apache Beam Java SDK Mobile Gaming Examples - Dataflow'

t.intent 'Gets the Mobile-Gaming Example Code'
QuickstartArchetype.generate(t)

t.intent 'Runs the Mobile-Gaming Code with Direct runner'
StringBuilder cmd = new StringBuilder()

// Run the UserScore example
t.intent("Running: UserScore example on DataflowRunner")
//cmd.append("mvn compile exec:java ")
//        .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.UserScore ")
//        .append("""-Dexec.args="--tempLocation=gs://${t.gcsBucket()}/${t.gcsBucketSubDirectory()}/tmp
//             --output=java-userscore-result.txt
//             --input=gs://${t.gcsBucket()}/${t.gcsBucketSubDirectory()}/5000_gaming_data.csv\" """)
//        .append("-Pdataflow-runner")
cmd.append("mvn compile exec:java ")
        .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.UserScore ")
        .append("""-Dexec.args="--tempLocation=gs://yifan_auto_verification_test_bucket/temp
             --output=java-userscore-result_dataflow.txt
             --input=gs://yifan_auto_verification_test_bucket/5000_gaming_data.csv\" """)
        .append("-Pdataflow-runner")
t.run(cmd.toString())
//t.run "grep user19_BananaWallaby java-userscore-result.txt* "
//t.see "total_score: 231, user: user19_BananaWallaby"
t.intent("SUCCEED: UserScore successfully run on DirectRunners.")
cmd.setLength(0)


// Run the HourlyTeamScore example
t.intent("Running: HourlyTeamScore example on DataflowRunner")
//t.run "gsutil rm gs://${t.gcsBucket()}/${t.gcsBucketSubDirectory()}/java-hourlyteamscore-result.txt*"
//cmd.append("mvn compile exec:java ")
//        .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.HourlyTeamScore ")
//        .append("""-Dexec.args="--tempLocation=gs://${t.gcsBucket()}/${t.gcsBucketSubDirectory()}/tmp
//             --output=java-hourlyteamscore-result.txt
//             --input=gs://${t.gcsBucket()}/${t.gcsBucketSubDirectory()}/5000_gaming_data.csv\" """)
//        .append("-Pdataflow-runner")
cmd.append("mvn compile exec:java ")
        .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.HourlyTeamScore ")
        .append("""-Dexec.args="--tempLocation=gs://yifan_auto_verification_test_bucket/temp
             --output=java-hourlyteamscore-result_dataflow.txt
             --input=gs://yifan_auto_verification_test_bucket/5000_gaming_data.csv\" """)
        .append("-Pdataflow-runner")
t.run(cmd.toString())
//t.run "grep AzureBilby java-hourlyteamscore-result.txt* "
//t.see "total_score: 2788, team: AzureBilby"
t.intent("SUCCEED: HourlyTeamScore successfully run on DirectRunners.")
cmd.setLength(0)

t.done()