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
package org.apache.beam.sdk.io.clickhouse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Rule;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

/** Base setup for ClickHouse containers. */
public class BaseClickHouseTest {

  @Rule public Network network = Network.newNetwork();

  @Rule
  public GenericContainer zookeeper =
      new GenericContainer<>("zookeeper:3.4.13")
          .withExposedPorts(2181)
          .withNetwork(network)
          .withNetworkAliases("zookeeper");

  @Rule
  public ClickHouseContainer clickHouse =
      (ClickHouseContainer)
          new ClickHouseContainer()
              .withNetwork(network)
              .withClasspathResourceMapping(
                  "config.d/zookeeper_default.xml",
                  "/etc/clickhouse-server/config.d/zookeeper_default.xml",
                  BindMode.READ_ONLY);

  boolean executeSql(String sql) throws SQLException {
    try (Connection connection = clickHouse.createConnection("");
        Statement statement = connection.createStatement()) {
      return statement.execute(sql);
    }
  }

  ResultSet executeQuery(String sql) throws SQLException {
    try (Connection connection = clickHouse.createConnection("");
        Statement statement = connection.createStatement(); ) {
      return statement.executeQuery(sql);
    }
  }

  long executeQueryAsLong(String sql) throws SQLException {
    ResultSet rs = executeQuery(sql);
    rs.next();
    return rs.getLong(1);
  }
}
