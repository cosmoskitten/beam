package org.apache.beam.sdk.util;

import static com.google.api.client.util.BackOff.*;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link FlexibleBackoff}.
 */
@RunWith(JUnit4.class)
public class FlexibleBackoffTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private final FlexibleBackoff defaultBackoff = FlexibleBackoff.of();

  @Test
  public void testInvalidExponent() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("exponent -2.0 must be greater than 0");
    defaultBackoff.withExponent(-2.0);
  }

  @Test
  public void testInvalidInitialBackoff() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("initialBackoff PT0S must be at least 1 millisecond");
    defaultBackoff.withInitialBackoff(Duration.ZERO);
  }

  @Test
  public void testInvalidMaxBackoff() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("maxBackoff PT0S must be at least 1 millisecond");
    defaultBackoff.withMaxBackoff(Duration.ZERO);
  }

  @Test
  public void testInvalidMaxAttempts() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("maxAttempts 0 must be at least 1");
    defaultBackoff.withMaxAttempts(0);
  }

  @Test
  public void testInvalidCumulativeBackoff() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("maxCumulativeBackoff PT-0.002S must be at least 1 millisecond");
    defaultBackoff.withMaxCumulativeBackoff(Duration.millis(-2));
  }

  /** Tests with bounded interval, custom exponent, and unlimited retries. */
  @Test
  public void testBoundedIntervalWithReset() throws Exception {
    FlexibleBackoff backOff =
        FlexibleBackoff.of()
            .withInitialBackoff(Duration.millis(500))
            .withMaxBackoff(Duration.standardSeconds(1));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(374L), lessThan(1126L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));

    // Reset, should go back to short times.
    backOff.reset();
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(374L), lessThan(1126L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));

  }

  /** Tests with bounded interval, custom exponent, limited retries, and a reset. */
  @Test
  public void testMaxRetriesWithReset() throws Exception {
    FlexibleBackoff backOff =
        FlexibleBackoff.of()
            .withInitialBackoff(Duration.millis(500))
            .withMaxAttempts(1);
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), equalTo(STOP));
    assertThat(backOff.nextBackOffMillis(), equalTo(STOP));
    assertThat(backOff.nextBackOffMillis(), equalTo(STOP));
    assertThat(backOff.nextBackOffMillis(), equalTo(STOP));

    backOff.reset();
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), equalTo(STOP));
  }

  private static long countMaximumBackoff(FlexibleBackoff backOff) {
    long cumulativeBackoffMillis = 0;
    long currentBackoffMillis = backOff.nextBackOffMillis();
    while (currentBackoffMillis != STOP) {
      cumulativeBackoffMillis += currentBackoffMillis;
      currentBackoffMillis = backOff.nextBackOffMillis();
    }
    return cumulativeBackoffMillis;
  }

  /** Tests with bounded interval, custom exponent, limited cumulative time, and a reset. */
  @Test
  public void testBoundedIntervalAndCumTimeWithReset() throws Exception {
    FlexibleBackoff backOff =
        FlexibleBackoff.of()
            .withInitialBackoff(Duration.millis(500))
            .withMaxBackoff(Duration.standardSeconds(1))
            .withMaxCumulativeBackoff(Duration.standardMinutes(1));

    assertThat(countMaximumBackoff(backOff), equalTo(Duration.standardMinutes(1).getMillis()));

    backOff.reset();
    assertThat(countMaximumBackoff(backOff), equalTo(Duration.standardMinutes(1).getMillis()));
    // sanity check: should get 0 if we don't reset
    assertThat(countMaximumBackoff(backOff), equalTo(0L));

    backOff.reset();
    assertThat(countMaximumBackoff(backOff), equalTo(Duration.standardMinutes(1).getMillis()));
  }

  /**
   * Tests with bounded interval, custom exponent, limited cumulative time and attempts.
   */
  @Test
  public void testBoundedIntervalAndCumTimeAndRetriesWithReset() throws Exception {
    FlexibleBackoff backOff =
        FlexibleBackoff.of()
            .withInitialBackoff(Duration.millis(500))
            .withMaxBackoff(Duration.standardSeconds(1))
            .withMaxCumulativeBackoff(Duration.standardMinutes(1));

    long cumulativeBackoffMillis = 0;
    long currentBackoffMillis = backOff.nextBackOffMillis();
    while (currentBackoffMillis != STOP) {
      cumulativeBackoffMillis += currentBackoffMillis;
      currentBackoffMillis = backOff.nextBackOffMillis();
    }
    assertThat(cumulativeBackoffMillis, equalTo(Duration.standardMinutes(1).getMillis()));
  }
}