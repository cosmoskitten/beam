package org.apache.beam.examples.complete.game.utils;

import java.util.TimeZone;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Shared constants between game series classes.
 */
public class GameConstants {

  public static final String TIMESTAMP_ATTRIBUTE = "timestamp_ms";

  public static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
          .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));
}
