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
package org.apache.beam.runners.spark.translation;

import static org.junit.Assert.assertEquals;

import java.nio.charset.Charset;
import org.junit.Test;

/** Unit tests of {@link CustomSparkHashPartitioner}. */
public class CustomSparkHashPartitionerTest {

  final CustomSparkHashPartitioner partitioner = CustomSparkHashPartitioner.of(2);

  @Test
  public void testPartitioningWithBytes() {
    byte[] key1 = "Test1".getBytes(Charset.defaultCharset());
    byte[] key2 = "Test1".getBytes(Charset.defaultCharset());
    final int partitionKey1 = partitioner.getPartition(key1);
    final int partitionKey2 = partitioner.getPartition(key2);
    assertEquals(partitionKey1, partitionKey2);
  }

  @Test
  public void testPartitioningWithStrings() {
    final int partitionKey1 = partitioner.getPartition("Test1");
    final int partitionKey2 = partitioner.getPartition("Test1");
    assertEquals(partitionKey1, partitionKey2);
  }

  @Test
  public void testPartitioningWithIntegers() {
    final int partitionKey1 = partitioner.getPartition(1);
    final int partitionKey2 = partitioner.getPartition(1);
    assertEquals(partitionKey1, partitionKey2);
  }
}
