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
package org.apache.beam.sdk.io.common;

import static org.apache.beam.sdk.io.common.IOITHelper.executeWithRetry;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/**
 * Tests for functions in {@link IOITHelper}.
 */
@RunWith(JUnit4.class)
public class IOITHelperTest {
  private static long startTimeMeasure;
  private static String message = "";
  private static ArrayList<Exception> listOfExceptionsThrown;

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Before
  public
  void setup(){
    listOfExceptionsThrown = new ArrayList<>();
  }

  @Test
  public void retryHealthyFunction() throws Exception {
    executeWithRetry(IOITHelperTest::validFunction);
    assertEquals(message, "This is healthy function.");
  }

  @Test
  public void retryFunctionThatWillFail() throws Exception {
    exceptionRule.expect(SQLException.class);
    exceptionRule.expectMessage("Problem with connection");
    executeWithRetry(IOITHelperTest::failingFunction);
    assertEquals(listOfExceptionsThrown.size(), 3);
  }

  @Test
  public void retryFunctionThatFailsWithMoreAttempts() throws Exception {
    exceptionRule.expect(SQLException.class);
    exceptionRule.expectMessage("Problem with connection");
    executeWithRetry(4, 1_000, IOITHelperTest::failingFunction);
    assertEquals(listOfExceptionsThrown.size(), 4);
  }

  @Test
  public void retryFunctionThatRecovers() throws Exception {
    startTimeMeasure = System.currentTimeMillis();
    System.out.println(startTimeMeasure);
    executeWithRetry(IOITHelperTest::recoveringFunction);
    assertEquals(listOfExceptionsThrown.size(), 1);
  }

  @Test
  public void retryFunctionThatRecoversAfterBiggerDelay() throws Exception {
    exceptionRule.expect(SQLException.class);
    exceptionRule.expectMessage("Problem with connection");
    executeWithRetry(3, 2_000, IOITHelperTest::recoveringFunctionWithBiggerDelay);
    assertEquals(listOfExceptionsThrown.size(), 1);
  }


  private static void failingFunction() throws SQLException {
    SQLException e = new SQLException("Problem with connection");
    listOfExceptionsThrown.add(e);
    throw e;
  }

  private static void recoveringFunction() throws SQLException {
    System.out.println(startTimeMeasure);
    if (System.currentTimeMillis() - startTimeMeasure < 1_001){
      SQLException e = new SQLException("Problem with connection");
      listOfExceptionsThrown.add(e);
      throw e;
    }
  }

  private static void recoveringFunctionWithBiggerDelay() throws SQLException {
    System.out.println(startTimeMeasure);
    if (System.currentTimeMillis() - startTimeMeasure < 2_001){
      SQLException e = new SQLException("Problem with connection");
      listOfExceptionsThrown.add(e);
      throw e;
    }
  }

  private static void validFunction() throws SQLException {
    message = "This is healthy function.";
  }
}
