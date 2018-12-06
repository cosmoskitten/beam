/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.gradle

import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.FileTree
import org.gradle.api.publish.maven.MavenPublication

/**
 * Usage:
 * <ul>
 *   <li>The version embedded in the vendored artifactId must match exactly the version of the vendored library with
 *   using '_' as a replacement for special characters like '.'.
 *   <li>The package relocation prefix should be 'org.apache.beam.vendor.canonical_library_name.version_identifier.'.
 *   <li>Upgrading the version of a vendored library should trigger a change in the artifact name.
 *   <li>Vendored artifact versioning starts at 0.1 and is decoupled from Apache Beam releases and version numbers.
 *   <li>Increment the vendored artifact version only if we need to release a new version.
 * </ul>
 *
 * <p>Example for com.google.guava:guava:20.0:
 * <ul>
 *   <li>groupId: org.apache.beam
 *   <li>artifactId: guava-20_0
 *   <li>namespace: org.apache.beam.vendor.guava.v20_0
 *   <li>version: 0.1
 * </ul>
 *
 * <p>See <a href="https://lists.apache.org/thread.html/4c12db35b40a6d56e170cd6fc8bb0ac4c43a99aa3cb7dbae54176815@%3Cdev.beam.apache.org%3E">
 * original email thread</a> for a discussion of the topic.
 */
class VendorJavaPlugin implements Plugin<Project> {

  static class VendorJavaPluginConfig {
    List<String> dependencies
    List<String> runtimeDependencies
    Map<String, String> relocations
    List<String> exclusions
    String groupId
    String artifactId
    String version
  }

  def isRelease(Project project) {
    return project.hasProperty('isRelease')
  }

  void apply(Project project) {

    // Deferred configuration so the extension is available; otherwise it
    // needs to happen in a task, but not sure where to attach that
    // task
    project.ext.vendorJava = {
      VendorJavaPluginConfig config = it ? it as VendorJavaPluginConfig : new VendorJavaPluginConfig()

      if (!isRelease(project)) {
        config.version += '-SNAPSHOT'
      }

      project.apply plugin: 'com.github.johnrengelman.shadow'

      project.apply plugin: 'java'

      // Register all Beam repositories and configuration tweaks
      Repositories.register(project)

      // Apply a plugin which provides tasks for dependency / property / task reports.
      // See https://docs.gradle.org/current/userguide/project_reports_plugin.html
      // for further details. This is typically very useful to look at the "htmlDependencyReport"
      // when attempting to resolve dependency issues.
      project.apply plugin: "project-report"

      project.dependencies {
        config.dependencies.each { compile it }
        config.runtimeDependencies.each { runtime it }
      }

      // Create a task which emulates the maven-archiver plugin in generating a
      // pom.properties file.
      def pomPropertiesFile = "${project.buildDir}/publications/mavenJava/pom.properties"
      project.task('generatePomPropertiesFileForMavenJavaPublication') {
        outputs.file "${pomPropertiesFile}"
        doLast {
          new File("${pomPropertiesFile}").text =
                  """version=${config.version}
groupId=${project.group}
artifactId=${project.name}
"""
        }
      }

      project.shadowJar {
        config.relocations.each { srcNamespace, destNamespace ->
          relocate(srcNamespace, destNamespace)
        }
        config.exclusions.each { exclude it }

        classifier = null
        mergeServiceFiles()
        zip64 true
      }

      project.task('validateVendoring', dependsOn: 'shadowJar') {
        inputs.files project.configurations.shadow.artifacts.files
        doLast {
          project.configurations.shadow.artifacts.files.each {
            FileTree exposedClasses = project.zipTree(it).matching {
              include "**/*.class"
              exclude "org/apache/beam/vendor/**"
              // BEAM-5919: Exclude paths for Java 9 multi-release jars.
              exclude "META-INF/versions/*/module-info.class"
              exclude "META-INF/versions/*/org/apache/beam/vendor/**"
            }
            if (exposedClasses.files) {
              throw new GradleException("$it exposed classes outside of org.apache.beam namespace: ${exposedClasses.files}")
            }
          }
        }
      }
      project.check.dependsOn 'validateVendoring'

      // Only publish vendored dependencies if specifically requested.
      if (project.hasProperty("vendoredDependenciesOnly")) {
        project.apply plugin: 'maven-publish'

        // Have the shaded jar include both the generate pom.xml and its properties file
        // emulating the behavior of the maven-archiver plugin.
        project.shadowJar {
          def pomFile = "${project.buildDir}/publications/mavenJava/pom-default.xml"

          // Validate that the artifacts exist before copying them into the jar.
          doFirst {
            if (!project.file("${pomFile}").exists()) {
              throw new GradleException("Expected ${pomFile} to have been generated by the 'generatePomFileForMavenJavaPublication' task.")
            }
            if (!project.file("${pomPropertiesFile}").exists()) {
              throw new GradleException("Expected ${pomPropertiesFile} to have been generated by the 'generatePomPropertiesFileForMavenJavaPublication' task.")
            }
          }

          dependsOn 'generatePomFileForMavenJavaPublication'
          into("META-INF/maven/${project.group}/${project.name}") {
            from "${pomFile}"
            rename('.*', 'pom.xml')
          }

          dependsOn project.generatePomPropertiesFileForMavenJavaPublication
          into("META-INF/maven/${project.group}/${project.name}") { from "${pomPropertiesFile}" }
        }

        project.publishing {
          repositories {
            maven {
              name "testPublicationLocal"
              url "file://${project.rootProject.projectDir}/testPublication/"
            }
            maven {
              url(project.properties['distMgmtSnapshotsUrl'] ?: isRelease(project)
                      ? 'https://repository.apache.org/service/local/staging/deploy/maven2'
                      : 'https://repository.apache.org/content/repositories/snapshots')

              // We attempt to find and load credentials from ~/.m2/settings.xml file that a user
              // has configured with the Apache release and snapshot staging credentials.
              // <settings>
              //   <servers>
              //     <server>
              //       <id>apache.releases.https</id>
              //       <username>USER_TOKEN</username>
              //       <password>PASS_TOKEN</password>
              //     </server>
              //     <server>
              //       <id>apache.snapshots.https</id>
              //       <username>USER_TOKEN</username>
              //       <password>PASS_TOKEN</password>
              //     </server>
              //   </servers>
              // </settings>
              def settingsXml = new File(System.getProperty('user.home'), '.m2/settings.xml')
              if (settingsXml.exists()) {
                def serverId = (project.properties['distMgmtServerId'] ?: isRelease(project)
                        ? 'apache.releases.https' : 'apache.snapshots.https')
                def m2SettingCreds = new XmlSlurper().parse(settingsXml).servers.server.find { server -> serverId.equals(server.id.text()) }
                if (m2SettingCreds) {
                  credentials {
                    username m2SettingCreds.username.text()
                    password m2SettingCreds.password.text()
                  }
                }
              }
            }
          }

          publications {
            mavenJava(MavenPublication) {
              groupId = config.groupId
              artifactId = config.artifactId
              version = config.version
              artifact project.shadowJar

              pom {
                name = project.description
                if (project.hasProperty("summary")) {
                  description = project.summary
                }
                url = "http://beam.apache.org"
                inceptionYear = "2016"
                licenses {
                  license {
                    name = "Apache License, Version 2.0"
                    url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                    distribution = "repo"
                  }
                }
                scm {
                  connection = "scm:git:https://gitbox.apache.org/repos/asf/beam.git"
                  developerConnection = "scm:git:https://gitbox.apache.org/repos/asf/beam.git"
                  url = "https://gitbox.apache.org/repos/asf?p=beam.git;a=summary"
                }
                issueManagement {
                  system = "jira"
                  url = "https://issues.apache.org/jira/browse/BEAM"
                }
                mailingLists {
                  mailingList {
                    name = "Beam Dev"
                    subscribe = "dev-subscribe@beam.apache.org"
                    unsubscribe = "dev-unsubscribe@beam.apache.org"
                    post = "dev@beam.apache.org"
                    archive = "http://www.mail-archive.com/dev%beam.apache.org"
                  }
                  mailingList {
                    name = "Beam User"
                    subscribe = "user-subscribe@beam.apache.org"
                    unsubscribe = "user-unsubscribe@beam.apache.org"
                    post = "user@beam.apache.org"
                    archive = "http://www.mail-archive.com/user%beam.apache.org"
                  }
                  mailingList {
                    name = "Beam Commits"
                    subscribe = "commits-subscribe@beam.apache.org"
                    unsubscribe = "commits-unsubscribe@beam.apache.org"
                    post = "commits@beam.apache.org"
                    archive = "http://www.mail-archive.com/commits%beam.apache.org"
                  }
                }
                developers {
                  developer {
                    name = "The Apache Beam Team"
                    email = "dev@beam.apache.org"
                    url = "http://beam.apache.org"
                    organization = "Apache Software Foundation"
                    organizationUrl = "http://www.apache.org"
                  }
                }
              }

              pom.withXml {
                // NB: This must come after asNode() logic, as it seems asNode()
                // removes XML comments.
                // TODO: Load this from file?
                def elem = asElement()
                def hdr = elem.getOwnerDocument().createComment(
                        '''
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at
        http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
  ''')
                elem.insertBefore(hdr, elem.getFirstChild())
              }
            }
          }
        }

        // Only sign artifacts if we are performing a release
        if (isRelease(project) && !project.hasProperty('noSigning')) {
          project.apply plugin: "signing"
          project.signing {
            useGpgCmd()
            sign project.publishing.publications
          }
        }
      }
    }
  }
}
