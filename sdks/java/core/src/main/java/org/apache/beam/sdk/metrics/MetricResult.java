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
package org.apache.beam.sdk.metrics;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.google.auto.value.AutoValue;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * The results of a single {@link MetricKey metric}.
 *
 * <p>Contains two implementations, @{@link Attempted} and {@link AttemptedAndCommitted}, for use
 * with runners that support "committed" metrics vs those that don't.
 *
 * <p>TODO(BEAM-6265): Decouple wire formats from internal formats, remove usage of MetricName.
 */
@Experimental(Kind.METRICS)
@JsonFilter("committedMetrics")
public abstract class MetricResult<T> {
  /** Return the name of the metric. */
  public MetricName getName() {
    return getKey().metricName();
  };

  public abstract MetricKey getKey();

  /**
   * {@link MetricResult} that only stores an "attempted" metric value; used by runners that don't
   * support "committed" metrics.
   *
   * <p>Also used as an intermediate step while constructing metrics that ultimately contain a
   * "committed" value as well.
   */
  @AutoValue
  public abstract static class Attempted<V> extends MetricResult<V> {
    @Override
    public abstract MetricKey getKey();

    @Nullable
    @Override
    protected V getCommitedOrNull() {
      return null;
    }

    @Override
    public boolean supportsCommitted() {
      return false;
    }

    @Override
    public abstract V getAttempted();

    @Override
    public Attempted<V> addAttempted(V update, BiFunction<V, V, V> combine) {
      return attempted(getKey(), combine.apply(getAttempted(), update));
    }

    @Override
    public AttemptedAndCommitted<V> addCommitted(V update, BiFunction<V, V, V> combine) {
      return create(getKey(), update, getAttempted());
    }

    @Override
    public <W> MetricResult<W> transform(Function<V, W> fn) {
      return MetricResult.attempted(getKey(), fn.apply(getAttempted()));
    }
  }

  /** {@link MetricResult} for runners that support both "attempted" and "committed" metrics. */
  @AutoValue
  public abstract static class AttemptedAndCommitted<V> extends MetricResult<V> {
    @Override
    public abstract MetricKey getKey();

    @Override
    public boolean supportsCommitted() {
      return true;
    }

    @Override
    protected abstract V getCommitedOrNull();

    @Override
    public abstract V getAttempted();

    @Override
    public <W> MetricResult<W> transform(Function<V, W> fn) {
      return MetricResult.create(getKey(), fn.apply(getCommitedOrNull()), fn.apply(getAttempted()));
    }
  }

  /**
   * Return the value of this metric across all successfully completed parts of the pipeline.
   *
   * <p>Not all runners will support committed metrics. If they are not supported, the runner will
   * throw an {@link UnsupportedOperationException}.
   */
  public T getCommitted() {
    T committed = getCommitedOrNull();
    if (committed == null) {
      throw new UnsupportedOperationException(
          "This runner does not currently support committed"
              + " metrics results. Please use 'attempted' instead.");
    }
    return committed;
  }

  public abstract boolean supportsCommitted();

  @Nullable
  protected abstract T getCommitedOrNull();

  /** Return the value of this metric across all attempts of executing all parts of the pipeline. */
  public abstract T getAttempted();

  public abstract <V> MetricResult<V> transform(Function<T, V> fn);

  private static <T> T combine(T l, T r, BiFunction<T, T, T> combine) {
    if (l == null) {
      return r;
    }
    if (r == null) {
      return l;
    }
    return combine.apply(l, r);
  }

  public MetricResult<T> addAttempted(T update, BiFunction<T, T, T> combine) {
    return create(getKey(), getCommitted(), combine.apply(getAttempted(), update));
  }

  public AttemptedAndCommitted<T> setCommitted(T update) {
    return create(getKey(), update, getAttempted());
  }

  public AttemptedAndCommitted<T> addCommitted(T update, BiFunction<T, T, T> combine) {
    return create(getKey(), combine.apply(getCommitted(), update), getAttempted());
  }

  public MetricResult<T> combine(MetricResult<T> other, BiFunction<T, T, T> combine) {
    return MetricResult.create(
        getKey(),
        MetricResult.combine(getCommitedOrNull(), other.getCommitedOrNull(), combine),
        MetricResult.combine(getAttempted(), other.getAttempted(), combine));
  }

  public static <T> AttemptedAndCommitted<T> create(MetricKey key, T committed, T attempted) {
    return new AutoValue_MetricResult_AttemptedAndCommitted<T>(key, committed, attempted);
  }

  public static <T> MetricResult<T> create(
      MetricKey key, Boolean isCommittedSupported, T attempted) {
    if (isCommittedSupported) {
      return create(key, attempted, attempted);
    } else {
      return attempted(key, attempted);
    }
  }

  public static <T> Attempted<T> attempted(MetricKey key, T attempted) {
    return new AutoValue_MetricResult_Attempted<T>(key, attempted);
  }
}
