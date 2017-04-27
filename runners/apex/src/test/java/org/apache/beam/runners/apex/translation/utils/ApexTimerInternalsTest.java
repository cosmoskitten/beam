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
package org.apache.beam.runners.apex.translation.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.datatorrent.lib.util.KryoCloneUtils;
import com.datatorrent.netlet.util.Slice;
import com.google.common.collect.Multimap;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.TimerInternals.TimerDataCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.TimeDomain;
import org.joda.time.Instant;
import org.junit.Test;

/**
 * Tests for {@link ApexTimerInternals}.
 */
public class ApexTimerInternalsTest {

  @Test
  public void testSerialization() {
    TimerDataCoder timerDataCoder = TimerDataCoder.of(GlobalWindow.Coder.INSTANCE);
    TimerData timerData = TimerData.of("arbitrary-id", StateNamespaces.global(),
        new Instant(0), TimeDomain.EVENT_TIME);
    String key = "key";
    ApexTimerInternals<String> timerInternals = new ApexTimerInternals<>(timerDataCoder);
    timerInternals.setContext(key, StringUtf8Coder.of(), Instant.now());
    timerInternals.setTimer(timerData);
    ApexTimerInternals<String> cloned;
    assertNotNull("Serialization", cloned = KryoCloneUtils.cloneObject(timerInternals));
    cloned.setContext(key, StringUtf8Coder.of(), Instant.now());
    Multimap<Slice, TimerData> timers = cloned.getTimersReadyToProcess(new Instant(1).getMillis());
    assertEquals(1, timers.size());
  }

}
