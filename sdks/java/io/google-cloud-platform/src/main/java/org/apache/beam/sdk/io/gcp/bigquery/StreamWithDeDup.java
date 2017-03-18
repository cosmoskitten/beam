package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.JsonSchemaToTableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
* PTransform that performs streaming BigQuery write. To increase consistency,
* it leverages BigQuery best effort de-dup mechanism.
 */
class StreamWithDeDup<T> extends PTransform<PCollection<T>, PDone> {
  private final Write<T> write;

  /** Constructor. */
  StreamWithDeDup(Write<T> write) {
    this.write = write;
  }

  @Override
  protected Coder<Void> getDefaultOutputCoder() {
    return VoidCoder.of();
  }

  @Override
  public PDone expand(PCollection<T> input) {
    // A naive implementation would be to simply stream data directly to BigQuery.
    // However, this could occasionally lead to duplicated data, e.g., when
    // a VM that runs this code is restarted and the code is re-run.

    // The above risk is mitigated in this implementation by relying on
    // BigQuery built-in best effort de-dup mechanism.

    // To use this mechanism, each input TableRow is tagged with a generated
    // unique id, which is then passed to BigQuery and used to ignore duplicates.

    PCollection<KV<ShardedKey<String>, TableRowInfo>> tagged =
        input.apply(ParDo.of(new TagWithUniqueIdsAndTable<T>(
            input.getPipeline().getOptions().as(BigQueryOptions.class), write.getTable(),
            write.getTableRefFunction(), write.getFormatFunction())));

    // To prevent having the same TableRow processed more than once with regenerated
    // different unique ids, this implementation relies on "checkpointing", which is
    // achieved as a side effect of having StreamingWriteFn immediately follow a GBK,
    // performed by Reshuffle.
    NestedValueProvider<TableSchema, String> schema =
        write.getJsonSchema() == null
            ? null
            : NestedValueProvider.of(write.getJsonSchema(), new JsonSchemaToTableSchema());
    tagged
        .setCoder(KvCoder.of(ShardedKeyCoder.of(StringUtf8Coder.of()), TableRowInfoCoder.of()))
        .apply(Reshuffle.<ShardedKey<String>, TableRowInfo>of())
        .apply(
            ParDo.of(
                new StreamingWriteFn(
                    schema,
                    write.getCreateDisposition(),
                    write.getTableDescription(),
                    write.getBigQueryServices())));

    // Note that the implementation to return PDone here breaks the
    // implicit assumption about the job execution order. If a user
    // implements a PTransform that takes PDone returned here as its
    // input, the transform may not necessarily be executed after
    // the BigQueryIO.Write.

    return PDone.in(input.getPipeline());
  }
}
