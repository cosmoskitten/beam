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

package org.apache.beam.runners.gearpump.translators.utils;

import static org.junit.Assert.assertNull;

import org.apache.gearpump.cluster.ClientToMaster;
import org.apache.gearpump.cluster.MasterToClient;
import org.apache.gearpump.cluster.client.ClientContext;

import java.util.concurrent.TimeUnit;

import scala.concurrent.Await;
import scala.concurrent.duration.FiniteDuration;

/**
 * utility functions for tests.
 */
public class TestUtils {

  public static void checkFailure(ClientContext clientContext) throws Exception {
    MasterToClient.LastFailure failure = (MasterToClient.LastFailure)
        Await.result(clientContext.askAppMaster(1, new ClientToMaster.GetLastFailure(1)),
            new FiniteDuration(15, TimeUnit.MILLISECONDS));
    assertNull(failure.error());
  }
}
