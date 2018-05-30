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

class dependency_check_utils {
/**
 * Returns a boolean that indicates whether the dependency is out-of-date.
 *
 * @param currentVersion the version used by Beam
 * @param latestVersion the version found in public repositories such as maven central repo and PyPI
 * */
    private static def compareDependencyVersions(String currentVersion, String latestVersion) {
        def currentVersionSplit = currentVersion.tokenize('.')
        def latestVersionSplit = latestVersion.tokenize('.')
        def minLength = Math.min(currentVersionSplit.size(), latestVersionSplit.size())
        // compare major versions
        if (minLength > 0 && currentVersionSplit[0] < latestVersionSplit[0]) {
            return true
        }
        // compare sub versions
        if (minLength > 1 && latestVersionSplit[1] - currentVersionSplit[0] >= "3") {
            return true
        }
        return false
    }

/**
 * Extracts dependency check outputs and analyze deps' versions.
 * Returns a collection of dependencies which is far behind the latest version:
 * 1. dependency has major release. e.g org.assertj:assertj-core [2.5.0 -> 3.10.0]
 * 2. dependency is 3 sub-versions behind the newest one. e.g org.tukaani:xz [1.5 -> 1.8]
 *
 * @param file the path of the dependency check report to filter on.
 * */
    private static def parseDependencyResult(String file) {
        File report = new File(file)
        List<String> outdatedDeps = new ArrayList<>()
        if (!report.exists()) {
            print "Cannot fine dependency check report at ${file}"
            return outdatedDeps
        } else {
            boolean findOutdatedDependency = false
            report.eachLine { line ->
                if (line.contains("The following dependencies have later release versions:")) {
                    findOutdatedDependency = true
                } else if (findOutdatedDependency) {
                    def versions = line.substring(line.indexOf("[") + 1, line.indexOf("]")).split()
                    if (compareDependencyVersions(versions[0], versions[2])) {
                        outdatedDeps.add(line)
                    }
                }
            }
        }
        return outdatedDeps
    }

/**
 * Returns a string report contains dependencies of Java and Python,
 * which are outdated and need to be taking care of.
 *
 * */
    static def generateDependencyReport() {
//        def resultPath = '../../build/dependencyUpdates/'
//        StringBuilder report = new StringBuilder()
//        def javaOutdated = parseDependencyResult("${resultPath}report.txt")
//        def resultSummary = """
//        -------------------------------------------\n
//        Beam Dependency Check Report\n
//        --------------------------------------------\n\n
//      """
//        report.append(resultSummary)
//                .append("Outdated Java dependencies: \n")
//        javaOutdated.forEach { dep ->
//            report.append(dep).append("\n")
//        }
//        println report.toString()
        println 'lalalalalalalalala'
    }
}