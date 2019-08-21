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

import CommonTestProperties.SDK
import DockerPublisher

class SDKHarnessPublisher {
  private DockerPublisher publisher

  SDKHarnessPublisher(String repositoryRoot = 'gcr.io/apache-beam-testing/beam_portability') {
    this.publisher = new DockerPublisher(repositoryRoot)
  }

  /**
   * Builds the SDK harness image and pushes it to the registry.
   *
   * @param job - jenkins job
   * @param sdk - SDK for which the SDK harness image will be published
   */
  final void publishSDKHarness(job, SDK sdk) {
    String sdkName = sdk.name().toLowerCase()
    publisher.publish(job, ":sdks:${sdkName}:container:docker", sdkName)
  }

  /**
   * Returns the name of the SDK harness image in the following format: <repositoryRoot>/<imageName>:<imageTag>
   */
  final String getFullImageName(SDK sdk) {
    String sdkName = sdk.name().toLowerCase()
    return publisher.getFullImageName(sdkName)
  }
}
