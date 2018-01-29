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

package org.apache.beam.sdk.extensions.sql.impl.rule;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.BeamRecord;

/**
 * AggregateWindowField.
 */
@Internal
@AutoValue
public abstract class AggregateWindowField {
  private static final AggregateWindowField ABSENT = builder().build();
  public abstract int fieldIndex();
  public abstract @Nullable WindowFn<BeamRecord, ? extends BoundedWindow> windowFn();

  static Builder builder() {
    return new AutoValue_AggregateWindowField.Builder()
        .setFieldIndex(-1)
        .setWindowFn(null);
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setFieldIndex(int fieldIndex);
    abstract Builder setWindowFn(WindowFn<BeamRecord, ? extends BoundedWindow> window);
    abstract AggregateWindowField build();
  }

  public boolean isPresent() {
    return fieldIndex() >= 0;
  }

  public static AggregateWindowField absent() {
    return ABSENT;
  }
}
