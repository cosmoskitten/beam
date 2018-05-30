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


// This is the script for Beam dependency check that extracts the raw reports and prioritize updates
@Grab("com.google.cloud:google-cloud-bigquery:1.31.0")
String datasetName = "beam-dependency-states"
generateDependencyReport()


/**
 * Returns a boolean that indicates whether the dependency is out-of-date.
 *
 * @param currentVersion the version used by Beam
 * @param latestVersion the version found in public repositories such as maven central repo and PyPI
 * */
    private static def isOutdated(String currentVersion, String latestVersion) {
        def currentVersionSplit = currentVersion.tokenize('.')
        def latestVersionSplit = latestVersion.tokenize('.')
        def minLength = Math.min(currentVersionSplit.size(), latestVersionSplit.size())
        def currMajorVersion = currentVersionSplit[0]
        def latestMajorVersion = latestVersionSplit[0]
        def currMinorVersion = minLength > 1? currentVersionSplit[1] : null
        def latestMinorVersion = minLength > 1? latestVersionSplit[1] : null
        // compare major versions
        if (currMajorVersion != latestMajorVersion) {
          return true
        }
        // compare minor versions
        if(currMinorVersion != null && latestMinorVersion != null) {
            // Mark the dependency as high priority if there are non-numerical minor versions (e.g. 0-rc1 -> 0-rc3)
            if (!currMinorVersion.isNumber() || !latestMinorVersion.isNumber()) {
              return true
            } else if (Integer.parseInt(currMinorVersion) <= Integer.parseInt(latestMinorVersion) - 3) {
                return true
            }
        }
        return false
    }

/**
 * Extracts dependency check outputs and analyze deps' versions.
 * Returns a collection of dependencies which is far behind the latest version:
 * 1. dependency has major release. e.g org.assertj:assertj-core [2.5.0 -> 3.10.0]
 * 2. dependency is 3 sub-versions behind the newest one. e.g org.tukaani:xz [1.5 -> 1.8]
 * 3. TODO: dependency has not been updated for more than 6 months.
 *
 * @param file the path of the dependency check report to filter on.
 * */
    private static def parseDependencyResult(String file) {
        File report = new File(file)
        List<String> highPriorityDeps = new ArrayList<>()
        if (!report.exists()) {
            print "Cannot fine dependency check report at ${file}"
            return highPriorityDeps
        } else {
            boolean findOutdatedDependency = false
            report.eachLine { line ->
                if (line.contains("The following dependencies have later release versions:")) {
                    findOutdatedDependency = true
                } else if (findOutdatedDependency) {
                    def versions = line.substring(line.indexOf("[") + 1, line.indexOf("]")).split()
                    if (isOutdated(versions[0], versions[2])) {
                        highPriorityDeps.add(line)
                    }
                }
            }
        }
        return highPriorityDeps
    }


/**
 * Write report to a file. The file would be used as content of email notification.
 *
 * */
    static def writeReportToFile(String deps) {
//        File report = new File("src/build/dependencyUpdates/dependency-check-report.txt")
        File report  = new File("../../build/dependencyUpdates/dependency-check-report.txt")
        report.write "Beam Dependency Check Report"
        report << deps
    }


/**
 * Returns a string report contains dependencies of Java and Python,
 * which are outdated and need to be taking care of.
 *
 * */
    static def generateDependencyReport() {
//        def resultPath = 'src/build/dependencyUpdates/'
        def resultPath = '../../build/dependencyUpdates/'
        StringBuilder report = new StringBuilder()
        def javaHighPriorityDep = parseDependencyResult("${resultPath}report.txt")
        def resultSummary = """
        -------------------------------------------\n
        Java High Priority Dependency \n
        --------------------------------------------\n\n
      """
        report.append(resultSummary)
                .append("Outdated Java dependencies: \n")
        javaHighPriorityDep.each { dep ->
            report.append(dep).append("\n")
        }
        writeReportToFile(report.toString())
    }
