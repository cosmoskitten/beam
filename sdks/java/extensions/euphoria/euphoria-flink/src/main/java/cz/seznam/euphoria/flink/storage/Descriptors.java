/**
 * Copyright 2016 Seznam a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.storage;

import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;

/** Helper around storage descriptors. */
public class Descriptors {

  public static <T> ReducingStateDescriptor<T>
  from(ValueStorageDescriptor.MergingValueStorageDescriptor<T> descriptor) {
    return new ReducingStateDescriptor<T>(
        descriptor.getName(),
        new ReducingMerger<>(descriptor.getValueMerger()),
        descriptor.getValueClass());
  }

  /** Converts the given euphoria descriptor into its flink equivalent. */
  public static <T> ValueStateDescriptor<T> from(ValueStorageDescriptor<T> descriptor) {
    return new ValueStateDescriptor<>(
        descriptor.getName(),
        descriptor.getValueClass(),
        descriptor.getDefaultValue());
  }

  /** Converts the given euphoria descriptor into its flink equivalent. */
  public static <T> ListStateDescriptor<T> from(ListStorageDescriptor<T> descriptor) {
    return new ListStateDescriptor<>(
        descriptor.getName(),
        descriptor.getElementClass());
  }

  private Descriptors(){}
}
