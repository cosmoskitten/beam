package org.apache.beam.sdk;

import java.util.concurrent.ConcurrentLinkedQueue;

// TODO consider having a separate thread constantly flushing?
public class ConcurrentLogUtil {
  // Stores a bunch of logs in memory and then prints them out at the end of a test run.
  private static ConcurrentLinkedQueue<String> messages = new ConcurrentLinkedQueue<>();

  public static void Log(String msg) {
    messages.add(msg);
  }

  public static void FlushAndPrint() {
    String msg = messages.poll();
    while (msg != null) {
      System.out.println(msg);
      msg = messages.poll();
    }
  }
}
