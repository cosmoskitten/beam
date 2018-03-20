#!groovy
import java.text.SimpleDateFormat

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


class MobileGamingJavaUtils {

  public static final RUNNERS = [DirectRunner: "direct-runner",
    DataflowRunner: "dataflow-runner",
    SparkRunner: "spark-runner",
    ApexRunner: "apex-runner",
    FlinkRunner: "flink-runner"]

  public static final EXECUTION_TIMEOUT = 15

  // Lists used to verify team names generated in the LeaderBoard example
  public static final COLORS = new ArrayList<>(Arrays.asList(
    "Magenta",
    "AliceBlue",
    "Almond",
    "Amaranth",
    "Amber",
    "Amethyst",
    "AndroidGreen",
    "AntiqueBrass",
    "Fuchsia",
    "Ruby",
    "AppleGreen",
    "Apricot",
    "Aqua",
    "ArmyGreen",
    "Asparagus",
    "Auburn",
    "Azure",
    "Banana",
    "Beige",
    "Bisque",
    "BarnRed",
    "BattleshipGrey"))

  private static final USERSCORE_OUTPUT_PREFIX = "java-userscore-result-"

  private static final HOURLYTEAMSCORE_OUTPUT_PREFIX = "java-hourlyteamscore-result-"

  public static String generateCommand(String exampleName, String runner, TestScripts t){
    String commonArgs = "--tempLocation=gs://${t.gcsBucket()}/tmp --runner=${runner} "

    if(exampleName.equals("UserScore")){
      return generateUserScoreCommand(runner, t, commonArgs)
    } else if(exampleName.equals("HourlyTeamScore")){
      return generateHourlyTeamScoreCommand(runner, t, commonArgs)
    } else if(exampleName.equals("LeaderBoard")){
      return generateLeaderBoardCommand(runner, t, commonArgs)
    } else if(exampleName.equals("GameStats")){
      return generateGameStatsCommand(runner, t, commonArgs)
    } else if(exampleName.equals("Injector")){
      return generateInjectorCommand(t)
    }
    return "ERROR: Not found the example ${exampleName}."
}

  public static String getUserScoreOutputName(String runner){
    return "${USERSCORE_OUTPUT_PREFIX}${RUNNERS[runner]}.txt"
  }

  public static String getHourlyTeamScoreOutputName(String runner){
    return "${HOURLYTEAMSCORE_OUTPUT_PREFIX}${RUNNERS[runner]}.txt"
  }


  private static String generateUserScoreCommand(String runner, TestScripts t, String commonArgs){
    StringBuilder cmd = new StringBuilder()
    StringBuilder exeArgs = new StringBuilder(commonArgs)

    exeArgs.append("--input=gs://${t.gcsBucket()}/5000_gaming_data.csv ")
    if(runner == "DataflowRunner"){
      exeArgs.append("--project=${t.gcpProject()} ")
        .append("--output=gs://${t.gcsBucket()}/${getUserScoreOutputName(runner)} ")
    } else{
      exeArgs.append("--output=${getUserScoreOutputName(runner)} ")
    }

    cmd.append("mvn compile exec:java -q ")
      .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.UserScore ")
      .append("-Dexec.args=\"${exeArgs}\" ")
      .append("-P${RUNNERS[runner]}")
    return cmd
  }


  private static String generateHourlyTeamScoreCommand(String runner, TestScripts t, String commonArgs){
    StringBuilder cmd = new StringBuilder()
    StringBuilder exeArgs = new StringBuilder(commonArgs)

    exeArgs.append("--input=gs://${t.gcsBucket()}/5000_gaming_data.csv ")
    if(runner == "DataflowRunner"){
      exeArgs.append("--project=${t.gcpProject()} ")
        .append("--output=gs://${t.gcsBucket()}/${getHourlyTeamScoreOutputName(runner)} ")

    } else{
      exeArgs.append("--output=${getHourlyTeamScoreOutputName(runner)} ")
    }

    cmd.append("mvn compile exec:java -q ")
      .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.HourlyTeamScore ")
      .append("-Dexec.args=\"${exeArgs}\" ")
      .append("-P${RUNNERS[runner]}")
    return cmd
  }


  private static String generateLeaderBoardCommand(String runner, TestScripts t, String commonArgs){
    StringBuilder cmd = new StringBuilder()
    StringBuilder exeArgs = new StringBuilder(commonArgs)

    exeArgs.append("--project=${t.gcpProject()} ")
      .append("--dataset=${t.bqDataset()} ")
      .append("--topic=projects/${t.gcpProject()}/topics/${t.pubsubTopic()} ")
      .append("--output=gs://${t.gcsBucket()}/java-leaderboard-result.txt ")
      .append("--leaderBoardTableName=leaderboard_${runner} ")
      .append("--teamWindowDuration=5")

    cmd.append("mvn compile exec:java -q ")
      .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.LeaderBoard ")
      .append("-Dexec.args=\"${exeArgs}\" ")
      .append("-P${RUNNERS[runner]}")
    return cmd
  }


  private static String generateGameStatsCommand(String runner, TestScripts t, String commonArgs){
    StringBuilder cmd = new StringBuilder()
    StringBuilder exeArgs = new StringBuilder(commonArgs)

    exeArgs.append("--project=${t.gcpProject()} ")
      .append("--dataset=${t.bqDataset()} ")
      .append("--topic=projects/${t.gcpProject()}/topics/${t.pubsubTopic()} ")
      .append("--output=gs://${t.gcsBucket()}/java-leaderboard-result.txt ")
      .append("--fixedWindowDuration=5 ")
      .append("--userActivityWindowDuration=5 ")
      .append("--sessionGap=1 ")
      .append("--gameStatsTablePrefix=gamestats_${runner}")

    cmd.append("mvn compile exec:java -q ")
      .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.GameStats ")
      .append("-Dexec.args=\"${exeArgs}\" ")
      .append("-P${RUNNERS[runner]}")
    return cmd
}


  private static String generateInjectorCommand(TestScripts t){
    StringBuilder injectorCmd = new StringBuilder()
    injectorCmd.append("mvn compile exec:java ")
       .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.injector.Injector ")
       .append("-Dexec.args=\"${t.gcpProject()} ${t.pubsubTopic()} none\"")
    return injectorCmd
  }
}
