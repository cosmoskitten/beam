package com.google.cloud.dataflow.sdk.io;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.dataflow.sdk.io.CountingSource.NowTimestampFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.joda.time.Instant;

public class CountingInput {
  /**
   * Creates a {@link BoundedCountingInput} that will produce the specified number of elements,
   * from {@code 0} to {@code numElements - 1}.
   */
  public static PTransform<PBegin, PCollection<Long>> upTo(long numElements) {
    checkArgument(numElements > 0, "numElements (%s) must be greater than 0", numElements);
    return new BoundedCountingInput(numElements);
  }

  /**
   * Creates an {@link UnboundedCountingInput} that will produce numbers starting from {@code 0} up
   * to {@link Long#MAX_VALUE}.
   *
   * <p>After {@link Long#MAX_VALUE}, the transform never produces more output. (In practice, this
   * limit should never be reached.)
   *
   * <p>Elements in the resulting {@link PCollection PCollection&lt;Long&gt;} will by default have
   * timestamps corresponding to processing time at element generation, provided by
   * {@link Instant#now}. Use the transform returned by
   * {@link UnboundedCountingInput#withTimestampFn(SerializableFunction)} to control the output
   * timestamps.
   */
  public static PTransform<PBegin, PCollection<Long>> unbounded() {
    return new UnboundedCountingInput(new NowTimestampFn());
  }
  
  public static class BoundedCountingInput extends PTransform<PBegin, PCollection<Long>> {
    private final long numElements;

    private BoundedCountingInput(long numElements) {
      this.numElements = numElements;
    }
    
    @SuppressWarnings("deprecation")
    @Override
    public PCollection<Long> apply(PBegin begin) {
      return begin.apply(Read.from(CountingSource.upTo(numElements)));
    }
  }
  
  /**
   * A {@link PTransform} that will produce numbers starting from {@code 0} up to
   * {@link Long#MAX_VALUE}.
   *
   * <p>After {@link Long#MAX_VALUE}, the transform never produces more output. (In practice, this
   * limit should never be reached.)
   *
   * <p>Elements in the resulting {@link PCollection PCollection&lt;Long&gt;} will by default have
   * timestamps corresponding to processing time at element generation, provided by
   * {@link Instant#now}. Use the transform returned by
   * {@link UnboundedCountingInput#withTimestampFn(SerializableFunction)} to control the output
   * timestamps.
   */
  public static class UnboundedCountingInput extends PTransform<PBegin, PCollection<Long>> {
    private final SerializableFunction<Long, Instant> timestampFn;

    private UnboundedCountingInput(SerializableFunction<Long, Instant> timestampFn) {
      this.timestampFn = timestampFn;
    }

    /**
     * Returns an {@link UnboundedCountingInput} like this one, but where output elements have the
     * timestamp specified by the timestampFn.
     *
     * <p>Note that the timestamps produced by {@code timestampFn} may not decrease.
     */
    public UnboundedCountingInput withTimestampFn(SerializableFunction<Long, Instant> timestampFn) {
      return new UnboundedCountingInput(timestampFn);
    }

    @SuppressWarnings("deprecation")
    @Override
    public PCollection<Long> apply(PBegin begin) {
      return begin.apply(Read.from(CountingSource.unboundedWithTimestampFn(timestampFn)));
    }
  }
}

