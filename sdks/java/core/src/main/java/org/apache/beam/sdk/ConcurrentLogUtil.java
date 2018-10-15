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
package org.apache.beam.sdk;

import java.util.concurrent.ConcurrentLinkedQueue;

// TODO consider having a separate thread constantly flushing?
public class ConcurrentLogUtil implements java.io.Serializable {
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
