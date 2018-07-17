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
package org.apache.beam.sdk.extensions.sql.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test for {@link org.apache.beam.sdk.extensions.sql.jdbc.BeamSqlLine}. Note that this test only
 * tests for crashes (due to ClassNotFoundException for example). It does not test output.
 */
public class BeamSqlLineTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();
  public ByteArrayOutputStream byteArrayOutputStream;

  @Before
  public void setUp() {
    byteArrayOutputStream = new ByteArrayOutputStream();
  }

  @Test
  public void testSqlLine_emptyArgs() throws Exception {
    BeamSqlLine.main(new String[] {});
  }

  @Test
  public void testSqlLine_nullCommand() throws Exception {
    BeamSqlLine.main(new String[] {"-e", ""});
  }

  @Test
  public void testSqlLine_simple() throws Exception {
    BeamSqlLine.main(new String[] {"-e", "SELECT 1;"});
  }

  @Test
  public void testSqlLine_parse() throws Exception {
    BeamSqlLine.main(new String[] {"-e", "SELECT 'beam';"});
  }

  @Test
  public void testSqlLine_ddl() throws Exception {
    BeamSqlLine.main(
        new String[] {
          "-e", "CREATE TABLE test (id INTEGER) TYPE 'text';", "-e", "DROP TABLE test;"
        });
  }

  @Test
  public void classLoader_readFile() throws Exception {
    File simpleTable = folder.newFile();

    BeamSqlLine.main(
        new String[] {
          "-e",
          "CREATE TABLE test_table (id INTEGER) TYPE 'text' LOCATION '"
              + simpleTable.getAbsolutePath()
              + "';",
          "-e",
          "SELECT * FROM test_table;",
          "-e",
          "DROP TABLE test_table;"
        });
  }

  @Test
  public void testSqlLine_select() throws Exception {
    BeamSqlLine.testMain(
        new String[] {"-e", "SELECT 3, 'hello', DATE '2018-05-28';"},
        new PrintStream(byteArrayOutputStream));

    verifyStringOutputTrue(byteArrayOutputStream.toString(), "3", "hello", "2018-05-28");
    verifyStringOutputFalse(byteArrayOutputStream.toString(), "4", "hella", "2018-09-09");
  }

  @Test
  public void testSqlLine_selectFromTable() throws Exception {
    BeamSqlLine.testMain(
        new String[] {
          "-e",
          "CREATE TABLE table_test (col_a VARCHAR, col_b VARCHAR, col_c VARCHAR, col_x TINYINT, col_y INT, col_z BIGINT) TYPE 'test';",
          "-e",
          "INSERT INTO table_test VALUES ('a', 'b', 'c', 1, 2, 3);",
          "-e",
          "SELECT * FROM table_test;"
        },
        new PrintStream(byteArrayOutputStream));

    verifyStringOutputTrue(
        byteArrayOutputStream.toString(), "col_a", "col_b", "col_c", "col_x", "col_y", "col_z");
    verifyStringOutputTrue(byteArrayOutputStream.toString(), "a", "b", "c", "1", "2", "3");
  }

  @Test
  public void testSqlLine_insertSelect() throws Exception {
    BeamSqlLine.testMain(
        new String[] {
          "-e",
          "CREATE TABLE table_test (col_a VARCHAR, col_b VARCHAR) TYPE 'test';",
          "-e",
          "INSERT INTO table_test SELECT '3', 'hello';",
          "-e",
          "SELECT * FROM table_test;"
        },
        new PrintStream(byteArrayOutputStream));

    verifyStringOutputTrue(byteArrayOutputStream.toString(), "3", "hello");
  }

  @Test
  public void testSqlLine_GroupBy() throws Exception {
    BeamSqlLine.testMain(
        new String[] {
          "-e",
          "CREATE TABLE table_test (col_a VARCHAR, col_b VARCHAR) TYPE 'test';",
          "-e",
          "INSERT INTO table_test SELECT '3', 'foo';",
          "-e",
          "INSERT INTO table_test SELECT '3', 'bar';",
          "-e",
          "INSERT INTO table_test SELECT '4', 'foo';",
          "-e",
          "SELECT col_a, count(*) FROM table_test GROUP BY col_a;"
        },
        new PrintStream(byteArrayOutputStream));

    // col_a 3 has count(*) = 2
    verifyStringOutputTrue(byteArrayOutputStream.toString(), "3", "2");
    // col_a 4 has count(*) = 1
    verifyStringOutputTrue(byteArrayOutputStream.toString(), "4", "1");
  }

  @Test
  public void testSqlLine_fixedWindow() throws Exception {
    BeamSqlLine.testMain(
        new String[] {
          "-e",
          "CREATE TABLE table_test (col_a VARCHAR, col_b TIMESTAMP) TYPE 'test';",
          "-e",
          "INSERT INTO table_test SELECT '3', TIMESTAMP '2018-07-01 21:26:06';",
          "-e",
          "INSERT INTO table_test SELECT '3', TIMESTAMP '2018-07-01 21:26:07';",
          "-e",
          "SELECT TUMBLE_START(col_b, INTERVAL '1' SECOND), count(*) FROM table_test GROUP BY TUMBLE(col_b, INTERVAL '1' SECOND);"
        },
        new PrintStream(byteArrayOutputStream));

    verifyStringOutputTrue(byteArrayOutputStream.toString(), "2018-07-01 21:26:06", "1");
    verifyStringOutputTrue(byteArrayOutputStream.toString(), "2018-07-01 21:26:07", "1");
  }

  @Test
  public void testSqlLine_slidingWindow() throws Exception {
    BeamSqlLine.testMain(
        new String[] {
          "-e",
          "CREATE TABLE table_test (col_a VARCHAR, col_b TIMESTAMP) TYPE 'test';",
          "-e",
          "INSERT INTO table_test SELECT '3', TIMESTAMP '2018-07-01 21:26:06';",
          "-e",
          "INSERT INTO table_test SELECT '4', TIMESTAMP '2018-07-01 21:26:07';",
          "-e",
          "INSERT INTO table_test SELECT '6', TIMESTAMP '2018-07-01 21:26:08';",
          "-e",
          "INSERT INTO table_test SELECT '7', TIMESTAMP '2018-07-01 21:26:09';",
          "-e",
          "SELECT HOP_END(col_b, INTERVAL '1' SECOND, INTERVAL '2' SECOND), count(*) FROM table_test GROUP BY HOP(col_b, INTERVAL '1' SECOND, INTERVAL '2' SECOND);"
        },
        new PrintStream(byteArrayOutputStream));

    verifyStringOutputTrue(byteArrayOutputStream.toString(), "2018-07-01 21:26:07", "1");
    verifyStringOutputTrue(byteArrayOutputStream.toString(), "2018-07-01 21:26:08", "2");
    verifyStringOutputTrue(byteArrayOutputStream.toString(), "2018-07-01 21:26:09", "2");
    verifyStringOutputTrue(byteArrayOutputStream.toString(), "2018-07-01 21:26:10", "2");
    verifyStringOutputTrue(byteArrayOutputStream.toString(), "2018-07-01 21:26:11", "1");
  }

  private void verifyStringOutputTrue(String queryOutput, String... strs) {
    for (String str : strs) {
      Assert.assertTrue(queryOutput.contains(str));
    }
  }

  private void verifyStringOutputFalse(String queryOutput, String... strs) {
    for (String str : strs) {
      Assert.assertFalse(queryOutput.contains(str));
    }
  }
}
