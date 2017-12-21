package org.apache.beam.runners.core.construction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Tests for UrnUtils.
 */
public class UrnUtilsTest {

  private static final String GOOD_URN = "org.apache.beam:coder:bytes:v1";
  private static final String MISSING_URN = "org.apache.beam:fake:v1";
  private static final String BAD_URN = "Beam";

  @Test
  public void testGoodUrnSuccedes() {
    assertEquals(GOOD_URN, UrnUtils.validateCommonUrn(GOOD_URN));
  }

  @Test
  public void testMissingUrnFails() {
    try {
      UrnUtils.validateCommonUrn(MISSING_URN);
      fail("Should have rejected " + MISSING_URN);
    } catch (IllegalArgumentException exn) {
      // expected
    }
  }

  @Test
  public void testBadUrnFails() {
    try {
      UrnUtils.validateCommonUrn(BAD_URN);
      fail("Should have rejected " + BAD_URN);
    } catch (IllegalArgumentException exn) {
      // expected
    }
  }
}
