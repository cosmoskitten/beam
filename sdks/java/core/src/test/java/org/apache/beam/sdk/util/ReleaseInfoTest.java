package org.apache.beam.sdk.util;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests for {@link ReleaseInfo}.
 */
public class ReleaseInfoTest {

  @Test
  public void getReleaseInfo() throws Exception {
    ReleaseInfo info = ReleaseInfo.getReleaseInfo();

    // Validate name
    assertThat(info.getName(), containsString("Beam"));

    // Validate semantic version
    String version = info.getVersion();
    String pattern = "\\d+\\.\\d+\\.\\d+.*";
    assertTrue(
        String.format("%s does not match pattern %s", version, pattern),
        version.matches(pattern));
  }
}
