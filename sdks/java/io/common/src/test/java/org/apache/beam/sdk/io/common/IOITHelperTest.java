package org.apache.beam.sdk.io.common;

import static org.apache.beam.sdk.io.common.IOITHelper.executeWithRetry;
import static org.junit.Assert.assertThat;

import java.sql.SQLException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/**
 * Tests for functions in {@link IOITHelper}.
 */
@RunWith(JUnit4.class)
public class IOITHelperTest {
  @Test
  public void retryFunctionThatWillFail(){
    try {
    executeWithRetry(IOITHelperTest::failingFunction);
      Assert.fail("Problem with connection")
    } catch (Exception e) {
      assertThat(e)
          .hasMessage()
    }
  }

  private static void failingFunction() throws SQLException {
    throw new SQLException("Problem with connection");
  }
}
