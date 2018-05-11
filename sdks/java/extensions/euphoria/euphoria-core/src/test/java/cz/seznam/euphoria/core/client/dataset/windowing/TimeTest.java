/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.dataset.windowing;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import java.time.Duration;
import org.junit.Test;

public class TimeTest {

  @Test
  public void testWindowAssignment() {
    Time<?> windowing = Time.of(Duration.ofMillis(10));

    Iterable<TimeInterval> windows = windowing.assignWindowsToElement(new TimestampedElement<>(11));

    assertEquals(1, Iterables.size(windows));
    assertEquals(new TimeInterval(10, 20), Iterables.getOnlyElement(windows));

    windows = windowing.assignWindowsToElement(new TimestampedElement<>(10));
    assertEquals(1, Iterables.size(windows));
    assertEquals(new TimeInterval(10, 20), Iterables.getOnlyElement(windows));

    windows = windowing.assignWindowsToElement(new TimestampedElement<>(9));
    assertEquals(1, Iterables.size(windows));
    assertEquals(new TimeInterval(0, 10), Iterables.getOnlyElement(windows));
  }
}
