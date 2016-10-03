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

package org.apache.beam.sdk.metrics;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CounterCell}.
 */
@RunWith(JUnit4.class)
public class CounterCellTest {

  private CounterCell cell = new CounterCell();

  @Test
  public void testDeltaAndCumulative() {
    cell.add(5);
    cell.add(7);
    assertThat(cell.getCumulative(), equalTo(12L));
    assertThat(cell.getCumulative(), equalTo(12L));

    assertThat(cell.getUpdateIfDirty(), equalTo(12L));
    assertThat("Without committing, update remains the same",
        cell.getUpdateIfDirty(), equalTo(12L));
    cell.commitUpdate();
    assertThat(cell.getUpdateIfDirty(), nullValue());
    assertThat("Cumulative is independent of dirty-state",
        cell.getCumulative(), equalTo(12L));

    cell.add(30);
    assertThat("Adding a new value also made the cell dirty",
        cell.getUpdateIfDirty(), equalTo(42L));
    assertThat(cell.getCumulative(), equalTo(42L));
  }

  @Test
  public void testIncrementBetweenGetAndCommit() {
    cell.add(5);
    assertThat(cell.getUpdateIfDirty(), equalTo(5L));

    cell.add(7);
    cell.commitUpdate();
    assertThat("Changes after getUpdateIfDirty are not committed",
        cell.getUpdateIfDirty(), equalTo(12L));
  }
}
