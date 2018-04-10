/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hcatalog;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;

/**
 * Helper for creating connection and test tables on hive database via JDBC driver.
 */

public class HiveDatabaseTestHelper {
  private static Connection con;
  private static String tableName;
  private static Statement stmt;

  HiveDatabaseTestHelper(String hiveHost,
                         Integer hivePort,
                         String hiveDatabase) throws SQLException {
    String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    String hiveUrl = String.format("jdbc:hive2://%s:%s/%s", hiveHost, hivePort, hiveDatabase);
    con = DriverManager.getConnection(hiveUrl, "", "");
    stmt = con.createStatement();
  }

  /**
   * Create hive table.
   */
  public String createHiveTable(String testIdentifier) throws SQLException {
    tableName = DatabaseTestHelper.getTestTableName(testIdentifier);
    stmt.execute(" CREATE TABLE IF NOT EXISTS " + tableName + " ( id STRING) ");
    return tableName;
  }


  /**
   * Delete hive table.
   */
  public void dropHiveTable() throws SQLException {
    stmt.execute(" DROP TABLE " + tableName);
  }

  public void closeConnection() throws SQLException {
    stmt.close();
    con.close();
  }
}
