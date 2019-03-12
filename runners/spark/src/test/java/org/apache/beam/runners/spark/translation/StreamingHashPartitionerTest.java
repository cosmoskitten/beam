package org.apache.beam.runners.spark.translation;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Unit tests of {@link StreamingHashPartitioner}. */
public class StreamingHashPartitionerTest {

  @Test
  public void testPartitioning() {
    final StreamingHashPartitioner partitioner = StreamingHashPartitioner.of(2);
    byte[] key1 = "Test1".getBytes();
    byte[] key2 = "Test1".getBytes();

    final int partitionKey1 = partitioner.getPartition(key1);
    final int partitionKey2 = partitioner.getPartition(key2);

    assertEquals(partitionKey1, partitionKey2);
  }
}
