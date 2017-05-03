package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>A key-preserving {@link DoFn} that explodes an iterable that has been grouped by key and
 * window.
 */
@Internal
public class BatchStatefulDoFn<K, V, OutputT>
    extends DoFn<KV<K, Iterable<KV<Instant, WindowedValue<KV<K, V>>>>>, OutputT> {

  private final DoFn<KV<K, V>, OutputT> underlyingDoFn;

  BatchStatefulDoFn(DoFn<KV<K, V>, OutputT> underlyingDoFn) {
    this.underlyingDoFn = underlyingDoFn;
  }

  public DoFn<KV<K, V>, OutputT> getUnderlyingDoFn() {
    return underlyingDoFn;
  }

  @ProcessElement
  public void processElement(final ProcessContext c, final BoundedWindow window) {
    throw new UnsupportedOperationException(
        "BatchStatefulDoFn.ProcessElement should never be invoked");
  }

  @Override
  public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
    return underlyingDoFn.getOutputTypeDescriptor();
  }
}
