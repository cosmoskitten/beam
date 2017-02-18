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
package org.apache.beam.runners.flink.translation.overrides.streaming;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Specialized expansion for
 * {@link org.apache.beam.sdk.transforms.View.AsSingleton View.AsSingleton} for the
 * Flink runner in streaming mode.
 */
public class FlinkStreamingViewAsSingleton<T>
    extends PTransform<PCollection<T>, PCollectionView<T>> {
  private View.AsSingleton<T> transform;

  /**
   * Builds an instance of this class from the overridden transform.
   */
  public FlinkStreamingViewAsSingleton(View.AsSingleton<T> transform) {
    this.transform = transform;
  }

  @Override
  public PCollectionView<T> expand(PCollection<T> input) {
    Combine.Globally<T, T> combine = Combine.globally(
        new SingletonCombine<>(transform.hasDefaultValue(), transform.defaultValue()));
    if (!transform.hasDefaultValue()) {
      combine = combine.withoutDefaults();
    }
    return input.apply(combine.asSingletonView());
  }

  @Override
  protected String getKindString() {
    return "StreamingViewAsSingleton";
  }

  private static class SingletonCombine<T> extends Combine.BinaryCombineFn<T> {
    private boolean hasDefaultValue;
    private T defaultValue;

    SingletonCombine(boolean hasDefaultValue, T defaultValue) {
      this.hasDefaultValue = hasDefaultValue;
      this.defaultValue = defaultValue;
    }

    @Override
    public T apply(T left, T right) {
      throw new IllegalArgumentException("PCollection with more than one element "
          + "accessed as a singleton view. Consider using Combine.globally().asSingleton() to "
          + "combine the PCollection into a single value");
    }

    @Override
    public T identity() {
      if (hasDefaultValue) {
        return defaultValue;
      } else {
        throw new IllegalArgumentException(
            "Empty PCollection accessed as a singleton view. "
                + "Consider setting withDefault to provide a default value");
      }
    }
  }
}
