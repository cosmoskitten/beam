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

package org.apache.beam.runners.samza.container;

import java.net.URI;
import java.util.Random;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigFactory;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.job.model.JobModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory for the Beam yarn container to load job model. */
public class ContainerCfgFactory implements ConfigFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerCfgFactory.class);

  private static final Object LOCK = new Object();
  static volatile JobModel jobModel;

  @Override
  public Config getConfig(URI configUri) {
    if (jobModel == null) {
      synchronized (LOCK) {
        if (jobModel == null) {
          String containerId = System.getenv(ShellCommandConfig.ENV_CONTAINER_ID());
          LOG.info(String.format("Got container ID: %s", containerId));
          String coordinatorUrl = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL());
          LOG.info(String.format("Got coordinator URL: %s", coordinatorUrl));
          int delay = new Random().nextInt(SamzaContainer.DEFAULT_READ_JOBMODEL_DELAY_MS()) + 1;
          jobModel = SamzaContainer.readJobModel(coordinatorUrl, delay);
        }
      }
    }

    return jobModel.getConfig();
  }
}
