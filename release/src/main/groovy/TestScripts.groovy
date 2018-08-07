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

import groovy.util.CliBuilder

/*
 * Scripting functions to make writing a test similar to the quickstart
 * instructions from https://beam.apache.org/get-started/quickstart-java/
 */
class TestScripts {

   // Global state to maintain when running the steps
   class globalStates {
     static File startDir
     static File curDir
     static String repoUrl
     static String ver
     static String gcpProject
     static String gcsBucket
     static String bqDataset
     static String pubsubTopic
   }

   def TestScripts(String[] args) {
     def cli = new CliBuilder()
     cli.ver(args:1, 'SDL Version')
     cli.repourl(args:1, 'Repository URL')
     cli.gcpProject(args:1, 'Google Cloud Project')
     cli.gcsBucket(args:1, 'Google Cloud Storage Bucket')
     cli.bqDataset(args:1, "BigQuery Dataset")
     cli.pubsubTopic(args:1, "PubSub Topic")

     def options = cli.parse(args)
     globalStates.repoUrl = options.repourl
     globalStates.ver = options.ver
     println "Repository URL: ${globalStates.repoUrl}"
     println "Version: ${globalStates.ver}"
     if (options.gcpProject) {
       globalStates.gcpProject = options.gcpProject
       println "GCS Project: ${globalStates.gcpProject}"
     }
     if (options.gcsBucket) {
       globalStates.gcsBucket = options.gcsBucket
       println "GCS Storage bucket: ${globalStates.gcsBucket}"
     }
     if (options.bqDataset) {
         globalStates.bqDataset = options.bqDataset
         println "BigQuery Dataset: ${globalStates.bqDataset}"
     }
     if (options.pubsubTopic) {
         globalStates.pubsubTopic = options.pubsubTopic
         println "PubSub Topic: ${globalStates.pubsubTopic}"
     }
   }

   def ver() {
     return globalStates.ver
   }

   def gcpProject() {
     return globalStates.gcpProject
   }

   def gcsBucket() {
     return globalStates.gcsBucket
   }

   def bqDataset() {
     return globalStates.bqDataset
   }

   def pubsubTopic() {
     return globalStates.pubsubTopic
   }

   // Both documents the overal scenario and creates a clean temp directory
   def describe(String desc) {
     globalStates.startDir = File.createTempDir()
     globalStates.startDir.deleteOnExit()
     globalStates.curDir = globalStates.startDir
     print "**************************************\n* Scenario: ${desc}\n**************************************\n"
   }

   // Just document the intention of a set of steps
   def intent(String desc) {
     print "\n**************************************\n* Test: ${desc}\n**************************************\n\n"
   }

   def success(String desc) {
     print "\n**************************************\n* SUCCESS: ${desc}\n**************************************\n\n"
   }

   // Run a command
   public String run(String cmd) {
     println cmd
     if (cmd.startsWith("cd ")) {
       _chdir(cmd.substring(3))
       return ""
     } else if (cmd.startsWith("mvn ")) {
       return _mvn(cmd.substring(4))
     } else {
       return _execute(cmd)
     }
   }

   // Check for expected results in actual stdout from previous command, if fails, log errors then exit.
   public void see(String expected, String actual) {
     if (!actual.contains(expected)) {
       globalStates.startDir.deleteDir()
       println "Cannot find ${expected} in ${actual}"
       error("Cannot find expected text")
     }
     println "Verified $expected"
   }

   // Check if there are one or more matches in stdout of the last command.
   public boolean seeAnyOf(List<String> expecteds, String actual) {
     for (String expected: expecteds) {
       if(actual.contains(expected)) {
         println "Verified $expected"
         return true
       }
     }
     println "Cannot find ${expecteds} in text"
     return false
   }

   // Cleanup and print success
   public void done() {
     globalStates.startDir.deleteDir()
     println "[SUCCESS]"
     System.exit(0)
   }

   // Run a single command, capture output, verify return code is 0
   private String _execute(String cmd) {
     def shell = "sh -c cmd".split(' ')
     shell[2] = cmd
     def pb = new ProcessBuilder(shell)
     pb.directory(globalStates.curDir)
     pb.redirectErrorStream(true)
     def proc = pb.start()
     String output_text = ""
     def text = StringBuilder.newInstance()
     proc.inputStream.eachLine {
       println it
       text.append(it + "\n")
     }
     proc.waitFor()
     output_text = text.toString().trim()
     if (proc.exitValue() != 0) {
       println output_text
       error("Failed command")
     }
     return output_text
   }

   // Change directory
   private void _chdir(String subdir) {
     globalStates.curDir = new File(globalStates.curDir.absolutePath, subdir)
     if (!globalStates.curDir.exists()) {
       error("Directory ${globalStates.curDir} not found")
     }
   }

   // Run a maven command, setting up a new local repository and a settings.xml with a custom repository
   private String _mvn(String args) {
     def m2 = new File(globalStates.startDir, ".m2/repository")
     m2.mkdirs()
     def settings = new File(globalStates.startDir, "settings.xml")
     settings.write """
       <settings>
         <localRepository>${m2.absolutePath}</localRepository>
           <profiles>
             <profile>
               <id>testrel</id>
                 <repositories>
                   <repository>
                     <id>test.release</id>
                     <url>${globalStates.repoUrl}</url>
                   </repository>
                 </repositories>
               </profile>
             </profiles>
        </settings>
        """
       def cmd = "mvn ${args} -s ${settings.absolutePath} -Ptestrel -B"
       String path = System.getenv("PATH");
       // Set the path on jenkins executors to use a recent maven
       // MAVEN_HOME is not set on some executors, so default to 3.5.2
       String maven_home = System.getenv("MAVEN_HOME") ?: '/home/jenkins/tools/maven/apache-maven-3.5.2'
       println "Using maven ${maven_home}"
       def mvnPath = "${maven_home}/bin"
       def setPath = "export PATH=${mvnPath}:${path} && "
       return _execute(setPath + cmd)
   }

   // Clean up and report error
   public void error(String text) {
     globalStates.startDir.deleteDir()
     println "[ERROR] $text"
     System.exit(1)
   }
}
