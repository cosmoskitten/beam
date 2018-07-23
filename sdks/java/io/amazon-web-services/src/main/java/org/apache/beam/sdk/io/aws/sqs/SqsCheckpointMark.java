package org.apache.beam.sdk.io.aws.sqs;

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.UnboundedSource;

@DefaultCoder(AvroCoder.class)
class SqsCheckpointMark implements UnboundedSource.CheckpointMark {

  private List<String> receiptHandlesToDelete;
  @AvroIgnore private SqsUnboundedReader reader;

  private SqsCheckpointMark() {
    this.receiptHandlesToDelete = new ArrayList<>();
  }

  public SqsCheckpointMark(SqsUnboundedReader reader, List<String> snapshotReceiptHandlesToDelete) {

    this.reader = reader;
    this.receiptHandlesToDelete = snapshotReceiptHandlesToDelete;
  }

  @Override
  public void finalizeCheckpoint() {
    for (String receiptHandle : receiptHandlesToDelete) {
      reader.delete(receiptHandle);
    }
  }
}
