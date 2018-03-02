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

    private static final TEAMSCORES = new ArrayList<>(Arrays.asList(
            "user7_AlmondMarmot,AlmondMarmot,9",
            "user2_AntiqueBrassPossum,AntiqueBrassPossum,15",
            "user15_AlmondMarmot,AlmondMarmot,1",
            "user9_BattleshipGreyBilby,BattleshipGreyBilby,4",
            "user9_AzureKoala,AzureKoala,17",
            "user6_BattleshipGreyBilby,BattleshipGreyBilby,8",
            "user4_FuchsiaKookaburra,FuchsiaKookaburra,5",
            "user6_BeigeKookaburra,BeigeKookaburra,7",
            "user0_BisqueCockatoo,BisqueCockatoo,2",
            "user0_AlmondQuokka,AlmondQuokka,18",
            "user0_BarnRedMarmot,BarnRedMarmot,15",
            "user4_AzureKoala,AzureKoala,7",
            "user8_BeigeKookaburra,BeigeKookaburra,19"
    ))

    public static String generateCommand(String exampleName, String runner, TestScripts t){
        if(exampleName.equals("UserScore")){
            return generateUserScoreCommand(runner, t)
        }
        if(exampleName.equals("HourlyTeamScore")){
            return generateHourlyTeamScoreCommand(runner, t)
        }
        if(exampleName.equals("LeaderBoard")){
            return generateLeaderBoardCommand(runner, t)
        }
        if(exampleName.equals("GameStats")){
            return generateGameStatsCommand(runner, t)
        }
        if(exampleName.equals("Injector")){
            return generateInjectorCommand(t)
        }
        return "ERROR: Not found the example ${exampleName}."
    }


    private static String generateUserScoreCommand(String runner, TestScripts t){
        StringBuilder cmd = new StringBuilder()
        StringBuilder exeArgs = new StringBuilder()

        exeArgs.append("--tempLocation=gs://${t.gcsBucket()}/tmp ")
                .append("--runner=${runner} ")
                .append("--input=gs://${t.gcsBucket()}/5000_gaming_data.csv ")
        if(runner == "DataflowRunner"){
            exeArgs.append("--project=${t.gcpProject()} ")
                    .append("--output=gs://${t.gcsBucket()}/java-userscore-result-${RUNNERS[runner]}.txt ")
        }
        else{
            exeArgs.append("--output=java-userscore-result-${RUNNERS[runner]}.txt ")
        }

        cmd.append("mvn compile exec:java -q ")
                .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.UserScore ")
                .append("-Dexec.args=\"${exeArgs.toString()}\" ")
                .append("-P${RUNNERS[runner]}")
        return cmd.toString()
    }


    private static String generateHourlyTeamScoreCommand(String runner, TestScripts t){
        StringBuilder cmd = new StringBuilder()
        StringBuilder exeArgs = new StringBuilder()

        exeArgs.append("--tempLocation=gs://${t.gcsBucket()}/tmp ")
                .append("--runner=${runner} ")
                .append("--input=gs://${t.gcsBucket()}/5000_gaming_data.csv ")
        if(runner == "DataflowRunner"){
            exeArgs.append("--project=${t.gcpProject()} ")
                    .append("--output=gs://${t.gcsBucket()}/java-hourlyteamscore-result-${RUNNERS[runner]}.txt ")

        }
        else{
            exeArgs.append("--output=java-hourlyteamscore-result-${RUNNERS[runner]}.txt ")
        }

        cmd.append("mvn compile exec:java -q ")
                .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.HourlyTeamScore ")
                .append("-Dexec.args=\"${exeArgs.toString()}\" ")
                .append("-P${RUNNERS[runner]}")
        return cmd.toString()
    }


    private static String generateLeaderBoardCommand(String runner, TestScripts t){
        StringBuilder cmd = new StringBuilder()
        StringBuilder exeArgs = new StringBuilder()

        exeArgs.append("--project=${t.gcpProject()} ")
                .append("--tempLocation=gs://${t.gcsBucket()}/tmp ")
                .append("--runner=${runner} ")
                .append("--dataset=${t.bqDataset()} ")
                .append("--topic=projects/${t.gcpProject()}/topics/${t.pubsubTopic()} ")
                .append("--output=gs://${t.gcsBucket()}/java-leaderboard-result.txt ")
                .append("--leaderBoardTableName=leaderboard_${runner} ")
                .append("--teamWindowDuration=5")

        cmd.append("mvn compile exec:java -q ")
                .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.LeaderBoard ")
                .append("-Dexec.args=\"${exeArgs.toString()}\" ")
                .append("-P${RUNNERS[runner]}")
        return cmd.toString()
    }


    private static String generateGameStatsCommand(String runner, TestScripts t){
        StringBuilder cmd = new StringBuilder()
        StringBuilder exeArgs = new StringBuilder()

        exeArgs.append("--project=${t.gcpProject()} ")
                .append("--tempLocation=gs://${t.gcsBucket()}/tmp ")
                .append("--runner=${runner} ")
                .append("--dataset=${t.bqDataset()} ")
                .append("--topic=projects/${t.gcpProject()}/topics/${t.pubsubTopic()} ")
                .append("--output=gs://${t.gcsBucket()}/java-leaderboard-result.txt ")
                .append("--fixedWindowDuration=5 ")
                .append("--userActivityWindowDuration=5 ")
                .append("--sessionGap=1 ")
                .append("--gameStatsTablePrefix=gamestats_${runner}")

        cmd.append("mvn compile exec:java -q ")
                .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.GameStats ")
                .append("-Dexec.args=\"${exeArgs.toString()}\" ")
                .append("-P${RUNNERS[runner]}")
        return cmd.toString()
    }


    private static String generateInjectorCommand(TestScripts t){
        StringBuilder injectorCmd = new StringBuilder()
        injectorCmd.append("mvn compile exec:java ")
                .append("-Dexec.mainClass=org.apache.beam.examples.complete.game.injector.Injector ")
                .append("-Dexec.args=\"${t.gcpProject()} ${t.pubsubTopic()} none\"")
        return injectorCmd.toString()
    }

    private static void simulateTeamScoreData(TestScripts t){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        Date currentTime = new Date()
        def readableDate = sdf.format(currentTime)
        def timeInMillis = currentTime.getTime()

        for(data in TEAMSCORES){
            t.run("gcloud pubsub topics publish ${t.pubsubTopic()} --attribute \"timestamp_ms\"=\"${timeInMillis}\" --message \"${data},${timeInMillis},${readableDate}\"")
        }

    }

    public static void update_gcloud(TestScripts t){
        t.run("gcloud -v")
        t.run("curl https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-189.0.0-linux-x86_64.tar.gz --output gcloud.tar.gz " +
                "&& tar xf gcloud.tar.gz")
        t.run("./google-cloud-sdk/install.sh --quiet")
        t.run(". ./google-cloud-sdk/path.bash.inc")
        t.run("gcloud components update --quiet || echo \'gcloud components update failed\'")
        t.run("gcloud -v")
    }
}
