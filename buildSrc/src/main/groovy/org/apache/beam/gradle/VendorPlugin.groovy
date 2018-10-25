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

class VendorPlugin implements Plugin<Project> {

  class VendorPluginExtension {
    String namespace = null;
  }

  void apply(Project project) {

    def extension = project.extensions.create('vendor', VendorPluginExtension)

    project.group = extension.group;

    project.repositories {
      maven { url project.offlineRepositoryRoot }

      // To run gradle in offline mode, one must first invoke
      // 'updateOfflineRepository' to create an offline repo
      // inside the root project directory. See the application
      // of the offline repo plugin within build_rules.gradle
      // for further details.
      if (project.gradle.startParameter.isOffline()) {
        return
      }

      mavenCentral()
      mavenLocal()
      jcenter()

      // Spring for resolving pentaho dependency.
      maven { url "https://repo.spring.io/plugins-release/" }

      // Release staging repository
      maven { url "https://oss.sonatype.org/content/repositories/staging/" }

      // Apache nightly snapshots
      maven { url "https://repository.apache.org/snapshots" }

      // Apache release snapshots
      maven { url "https://repository.apache.org/content/repositories/releases" }
    }

    // Apply a plugin which provides the 'updateOfflineRepository' task that creates an offline
    // repository. This offline repository satisfies all Gradle build dependencies and Java
    // project dependencies. The offline repository is placed within $rootDir/offline-repo
    // but can be overridden by specifying '-PofflineRepositoryRoot=/path/to/repo'.
    // Note that parallel build must be disabled when executing 'updateOfflineRepository'
    // by specifying '--no-parallel', see
    // https://github.com/mdietrichstein/gradle-offline-dependencies-plugin/issues/3
    project.apply plugin: "io.pry.gradle.offline_dependencies"
    project.offlineDependencies {
      repositories {
        mavenLocal()
        mavenCentral()
        jcenter()
        maven { url "https://plugins.gradle.org/m2/" }
        maven { url "http://repo.spring.io/plugins-release" }
        maven { url project.offlineRepositoryRoot }
      }
      includeSources = false
      includeJavadocs = false
      includeIvyXmls = false
    }

    // Apply a plugin which provides tasks for dependency / property / task reports.
    // See https://docs.gradle.org/current/userguide/project_reports_plugin.html
    // for further details. This is typically very useful to look at the "htmlDependencyReport"
    // when attempting to resolve dependency issues.
    project.apply plugin: "project-report"

    project.apply plugin: 'com.github.johnrengelman.shadow'

    shadow {
      dependencies {
        relocate("", extension.namespace)
      }
    }

    project.shadowJar({
      classifier = null
      mergeServiceFiles()
      zip64 true
    }

        project.task('validateShadedJarDoesntLeakNonOrgApacheBeamClasses', dependsOn: 'shadowJar') {
          inputs.files project.configurations.shadow.artifacts.files
          doLast {
            project.configurations.shadow.artifacts.files.each {
              FileTree exposedClasses = project.zipTree(it).matching {
                include "**/*.class"
                exclude "org/apache/beam/**"
              }
              if (exposedClasses.files) {
                throw new GradleException("$it exposed classes outside of org.apache.beam namespace: ${exposedClasses.files}")
              }
            }
          }
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
              artifact project.shadowJar

            }
          }
        }
  }
}

