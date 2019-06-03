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
package org.apache.beam.sdk.io.hcatalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.transforms.SerializableComparator;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;

/** Tests for {@link PartitionRangeTracker}. */
@RunWith(JUnit4.class)
public class PartitionRangeTrackerTest {

  public static final String TEST_DATABASE = "default";
  public static final String TEST_TABLE = "mytable";
  private ImmutableList<Partition> partitions;
  private final PartitionCreateTimeComparator partitionCreateTimeComparator =
      new PartitionCreateTimeComparator();

  @Rule public final ExpectedException expected = ExpectedException.none();

  @Rule
  public final transient TestRule testDataSetupRule =
      new TestWatcher() {
        @Override
        public Statement apply(final Statement base, final Description description) {
          return new Statement() {
            @Override
            public void evaluate() throws Throwable {
              if (description.getAnnotation(PartitionRangeTrackerTest.NeedsTestData.class)
                  != null) {
                prepareTestData();
              }
              base.evaluate();
            }
          };
        }
      };

  /** Use this annotation to setup partition data to be used in all tests. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD})
  private @interface NeedsTestData {}

  @Test
  @NeedsTestData
  public void testClaimingPartition() throws Exception {
    PartitionRange range = new PartitionRange(partitions, partitionCreateTimeComparator, null);
    PartitionRangeTracker tracker = new PartitionRangeTracker(range, partitionCreateTimeComparator);
    assertEquals(3, tracker.currentRestriction().getPartitions().size());
    assertEquals(partitions, tracker.currentRestriction().getPartitions());
    // Claim partition p1
    assertTrue(tracker.tryClaim(partitions.get(0)));
    // Claiming the same partition again, should not be allowed.
    assertFalse(tracker.tryClaim(partitions.get(0)));
  }

  @Test
  @NeedsTestData
  public void testClaimingPartitionInWrongOrder() throws Exception {
    PartitionRange range = new PartitionRange(partitions, partitionCreateTimeComparator, null);
    PartitionRangeTracker tracker = new PartitionRangeTracker(range, partitionCreateTimeComparator);
    assertTrue(tracker.tryClaim(partitions.get(1))); // createTime 14
    assertFalse(tracker.tryClaim(partitions.get(0))); // createTime 10
    assertFalse(tracker.tryClaim(partitions.get(2))); // createTime 13
  }

  private void prepareTestData() {
    Partition p1 = createPartition(new ArrayList<>(Arrays.asList("10", "20")), 10, 10);
    Partition p2 = createPartition(new ArrayList<>(Arrays.asList("10", "20")), 14, 14);
    Partition p3 = createPartition(new ArrayList<>(Arrays.asList("10", "20")), 13, 13);
    this.partitions = ImmutableList.of(p1, p2, p3);
  }

  private Partition createPartition(List<String> values, int createTime, int lastAccessTime) {
    return new Partition(values, TEST_DATABASE, TEST_TABLE, createTime, lastAccessTime, null, null);
  }

  private static class PartitionCreateTimeComparator implements SerializableComparator<Partition> {
    @Override
    public int compare(Partition o1, Partition o2) {
      if (o1.getCreateTime() > o2.getCreateTime()) {
        return -1;
      } else if (o1.getCreateTime() < o2.getCreateTime()) {
        return 1;
      }
      return 0;
    }
  }
}
