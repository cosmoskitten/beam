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

import org.junit.Test
import groovy.json.JsonSlurper
import static groovy.test.GroovyAssert.shouldFail

/**
 * Prober tests which performs health checks on deployed infrasture for
 * community metrics.
 */
class ProberTests {
  // TODO: Make this configurable
  def grafanaEndpoint = 'http://104.154.241.245';

  @Test
  void PingGrafanaHttpApi() {
    def allDashboardsJson = "${grafanaEndpoint}/api/search?type=dash-db".toURL().text
    def allDashboards = new JsonSlurper().parseText(allDashboardsJson)
    def dashboardNames = allDashboards.title
    // Validate at least one expected dashboard exists
    assert dashboardNames.contains('Post-commit Tests') : 'Expected dashboard does not exist'
    assert dashboardNames.size >= 3 : "Number of dashboards is less than expected ${dashboardNames}"
  }
}

