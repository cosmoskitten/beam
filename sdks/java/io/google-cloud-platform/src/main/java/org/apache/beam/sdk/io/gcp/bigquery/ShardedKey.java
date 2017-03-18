package org.apache.beam.sdk.io.gcp.bigquery;

/**
 * Created by relax on 3/17/17.
 */
class ShardedKey<K> {
  private final K key;
  private final int shardNumber;

  public static <K> ShardedKey<K> of(K key, int shardNumber) {
    return new ShardedKey<>(key, shardNumber);
  }

  ShardedKey(K key, int shardNumber) {
    this.key = key;
    this.shardNumber = shardNumber;
  }

  public K getKey() {
    return key;
  }

  public int getShardNumber() {
    return shardNumber;
  }
}
