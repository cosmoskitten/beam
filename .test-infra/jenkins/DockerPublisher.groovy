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

import CommonJobProperties as common

class DockerPublisher {
  private String repositoryRoot

  DockerPublisher(String repositoryRoot) {
    this.repositoryRoot = repositoryRoot
  }

  /**
   * Builds a Docker image from a gradle task and pushes it to the registry.
   *
   * @param job - jenkins job
   * @param gradleTask - name of a Gradle task
   * @param imageName - name of a docker image
   * @param imageTag - tag of a docker image
   */
  final void publish(job, String gradleTask, String imageName, String imageTag = 'latest') {
    build(job, gradleTask, imageTag)
    push(job, imageName, imageTag)
  }

  /**
   * Returns the name of a docker image in the following format: <repositoryRoot>/<imageName>:<imageTag>
   *
   * @param imageName - name of a docker image
   * @param imageTag - tag of a docker image
   */
  final String getFullImageName(String imageName, String imageTag = 'latest') {
    String image = "${repositoryRoot}/${imageName}"
    return "${image}:${imageTag}"
  }

  private void build(job, String gradleTask, String imageTag) {
    job.steps {
      gradle {
        rootBuildScriptDir(common.checkoutDir)
        common.setGradleSwitches(delegate)
        tasks(gradleTask)
        switches("-Pdocker-repository-root=${repositoryRoot}")
        switches("-Pdocker-tag=${imageTag}")
      }
    }
  }

  private void push(job, String imageName, String imageTag) {
    String image = "${repositoryRoot}/${imageName}"
    String targetImage = getFullImageName(imageName, imageTag)

    job.steps {
      shell("echo \"Tagging image\"...")
      shell("docker tag ${image} ${targetImage}")
      shell("echo \"Pushing image\"...")
      shell("docker push ${targetImage}")
    }
  }
}
