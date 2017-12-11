package org.apache.beam.sdk.io.kinesis;

/**
 * Internal exception thrown when shard end is encountered during iteration.
 */
class KinesisShardClosedException extends Exception {

  KinesisShardClosedException(String message) {
    super(message);
  }
}
