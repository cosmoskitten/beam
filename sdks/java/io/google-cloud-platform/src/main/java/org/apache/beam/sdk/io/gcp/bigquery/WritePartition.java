package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Partitions temporary files based on number of files and file sizes.
 */
class WritePartition extends DoFn<String, KV<Long, List<String>>> {
  private final PCollectionView<Iterable<KV<String, Long>>> resultsView;
  private TupleTag<KV<Long, List<String>>> multiPartitionsTag;
  private TupleTag<KV<Long, List<String>>> singlePartitionTag;

  public WritePartition(
      PCollectionView<Iterable<KV<String, Long>>> resultsView,
      TupleTag<KV<Long, List<String>>> multiPartitionsTag,
      TupleTag<KV<Long, List<String>>> singlePartitionTag) {
    this.resultsView = resultsView;
    this.multiPartitionsTag = multiPartitionsTag;
    this.singlePartitionTag = singlePartitionTag;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    List<KV<String, Long>> results = Lists.newArrayList(c.sideInput(resultsView));
    if (results.isEmpty()) {
      TableRowWriter writer = new TableRowWriter(c.element());
      writer.open(UUID.randomUUID().toString());
      results.add(writer.close());
    }

    long partitionId = 0;
    int currNumFiles = 0;
    long currSizeBytes = 0;
    List<String> currResults = Lists.newArrayList();
    for (int i = 0; i < results.size(); ++i) {
      KV<String, Long> fileResult = results.get(i);
      if (currNumFiles + 1 > Write.MAX_NUM_FILES
          || currSizeBytes + fileResult.getValue() > Write.MAX_SIZE_BYTES) {
        c.sideOutput(multiPartitionsTag, KV.of(++partitionId, currResults));
        currResults = Lists.newArrayList();
        currNumFiles = 0;
        currSizeBytes = 0;
      }
      ++currNumFiles;
      currSizeBytes += fileResult.getValue();
      currResults.add(fileResult.getKey());
    }
    if (partitionId == 0) {
      c.sideOutput(singlePartitionTag, KV.of(++partitionId, currResults));
    } else {
      c.sideOutput(multiPartitionsTag, KV.of(++partitionId, currResults));
    }
  }
}
