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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFns;
import org.apache.beam.sdk.util.state.AccumulatorCombiningState;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.util.state.ReadableState;
import org.apache.beam.sdk.util.state.State;
import org.apache.beam.sdk.util.state.StateContext;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.util.state.StateNamespace;
import org.apache.beam.sdk.util.state.StateTag;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.util.state.WatermarkHoldState;

import com.google.common.collect.Lists;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.joda.time.Instant;

import java.io.IOException;

class FlinkStateInternals implements StateInternals<Object> {

  private final AbstractStateBackend flinkStateBackend;

  public FlinkStateInternals(AbstractStateBackend flinkStateBackend) {
    this.flinkStateBackend = flinkStateBackend;
  }

  @Override
  public Object getKey() {
    return flinkStateBackend.getCurrentKey();
  }

  @Override
  public <T extends State> T state(StateNamespace namespace, StateTag<? super Object, T> address) {
    return address.bind(new StateTag.StateBinder<Object>() {
      @Override
      public <T> ValueState<T> bindValue(StateTag<? super Object, ValueState<T>> address, Coder<T> coder) {
        throw new UnsupportedOperationException("Here");
      }

      @Override
      public <T> BagState<T> bindBag(StateTag<? super Object, BagState<T>> address, Coder<T> elemCoder) {
        throw new UnsupportedOperationException("Here");
      }

      @Override
      public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT> bindCombiningValue(StateTag<? super Object, AccumulatorCombiningState<InputT, AccumT, OutputT>> address, Coder<AccumT> accumCoder, Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
        throw new UnsupportedOperationException("Here");
      }

      @Override
      public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT> bindKeyedCombiningValue(StateTag<? super Object, AccumulatorCombiningState<InputT, AccumT, OutputT>> address, Coder<AccumT> accumCoder, Combine.KeyedCombineFn<? super Object, InputT, AccumT, OutputT> combineFn) {
        throw new UnsupportedOperationException("Here");
      }

      @Override
      public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT> bindKeyedCombiningValueWithContext(StateTag<? super Object, AccumulatorCombiningState<InputT, AccumT, OutputT>> address, Coder<AccumT> accumCoder, CombineWithContext.KeyedCombineFnWithContext<? super Object, InputT, AccumT, OutputT> combineFn) {
        throw new UnsupportedOperationException("Here");
      }

      @Override
      public <W extends BoundedWindow> WatermarkHoldState<W> bindWatermark(StateTag<? super Object, WatermarkHoldState<W>> address, OutputTimeFn<? super W> outputTimeFn) {
        throw new UnsupportedOperationException("Here");
      }
    });
  }

  @Override
  public <T extends State> T state(final StateNamespace namespace, StateTag<? super Object, T> address, StateContext<?> c) {
    return address.bind(new StateTag.StateBinder<Object>() {
      @Override
      public <T> ValueState<T> bindValue(StateTag<? super Object, ValueState<T>> address, Coder<T> coder) {

        CoderTypeInformation<T> typeInfo = new CoderTypeInformation<>(coder);
        ValueStateDescriptor<T> stateDescriptor = new ValueStateDescriptor<>(address.getId(), typeInfo, null);
        final org.apache.flink.api.common.state.ValueState<T> state;
        try {
          state = flinkStateBackend.getPartitionedState(namespace.stringKey(), StringSerializer.INSTANCE, stateDescriptor);
        } catch (Exception e) {
          throw new RuntimeException("Error getting state.", e);
        }
        return new ValueState<T>() {
          @Override
          public void write(T input) {
            try {
              state.update(input);
            } catch (IOException e) {
              throw new RuntimeException("Error updating state.", e);
            }
          }

          @Override
          public ValueState<T> readLater() {
            return this;
          }

          @Override
          public T read() {
            try {
              return state.value();
            } catch (IOException e) {
              throw new RuntimeException("Error reading state.", e);
            }
          }

          @Override
          public void clear() {
            state.clear();
          }
        };
      }

      @Override
      public <T> BagState<T> bindBag(StateTag<? super Object, BagState<T>> address, Coder<T> elemCoder) {
        throw new UnsupportedOperationException("Here");

      }

      @Override
      public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT> bindCombiningValue(StateTag<? super Object, AccumulatorCombiningState<InputT, AccumT, OutputT>> address, Coder<AccumT> accumCoder, Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
        throw new UnsupportedOperationException("Here");
      }

      @Override
      public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT> bindKeyedCombiningValue(StateTag<? super Object, AccumulatorCombiningState<InputT, AccumT, OutputT>> address, Coder<AccumT> accumCoder, final Combine.KeyedCombineFn<? super Object, InputT, AccumT, OutputT> combineFn) {

        CoderTypeInformation<AccumT> typeInfo = new CoderTypeInformation<>(accumCoder);
        ValueStateDescriptor<AccumT> stateDescriptor = new ValueStateDescriptor<>(address.getId(), typeInfo, null);
        final org.apache.flink.api.common.state.ValueState<AccumT> state;
        try {
          state = flinkStateBackend.getPartitionedState(namespace.stringKey(), StringSerializer.INSTANCE, stateDescriptor);
        } catch (Exception e) {
          throw new RuntimeException("Error getting state." , e);
        }

        return new AccumulatorCombiningState<InputT, AccumT, OutputT>() {
          @Override
          public AccumT getAccum() {
            try {
              return state.value();
            } catch (IOException e) {
              throw new RuntimeException("Error reading state." ,e);
            }              }

          @Override
          public void addAccum(AccumT accum) {
            try {
              AccumT current = state.value();
              if (current == null) {
                state.update(accum);
              } else {
                current = combineFn.mergeAccumulators(flinkStateBackend.getCurrentKey(), Lists.newArrayList(current, accum));
                state.update(current);
              }
            } catch (IOException e) {
              throw new RuntimeException("Error reading state." ,e);
            }
          }

          @Override
          public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
            return combineFn.mergeAccumulators(flinkStateBackend.getCurrentKey(), accumulators);
          }

          @Override
          public AccumulatorCombiningState<InputT, AccumT, OutputT> readLater() {
            return this;
          }

          @Override
          public void add(InputT value) {
            try {
              AccumT current = state.value();
              if (current == null) {
                current = combineFn.createAccumulator(flinkStateBackend.getCurrentKey());
              }
              combineFn.addInput(flinkStateBackend.getCurrentKey(), current, value);
              state.update(current);
            } catch (IOException e) {
              throw new RuntimeException("Error reading state." ,e);
            }
          }

          @Override
          public ReadableState<Boolean> isEmpty() {
            try {
              AccumT current = state.value();
              if (current == null) {
                return new ReadableState<Boolean>() {
                  @Override
                  public Boolean read() {
                    return true;
                  }

                  @Override
                  public ReadableState<Boolean> readLater() {
                    return this;
                  }
                };
              } else {
                return new ReadableState<Boolean>() {
                  @Override
                  public Boolean read() {
                    return false;
                  }

                  @Override
                  public ReadableState<Boolean> readLater() {
                    return this;
                  }
                };
              }
            } catch (IOException e) {
              throw new RuntimeException("Error reading state.", e);
            }
          }

          @Override
          public OutputT read() {
            try {
              AccumT accum = state.value();
              return combineFn.extractOutput(flinkStateBackend.getCurrentKey(), accum);
            } catch (IOException e) {
              throw new RuntimeException("Error reading state.", e);
            }
          }

          @Override
          public void clear() {
            state.clear();
          }
        };
      }

      @Override
      public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT> bindKeyedCombiningValueWithContext(StateTag<? super Object, AccumulatorCombiningState<InputT, AccumT, OutputT>> address, Coder<AccumT> accumCoder, CombineWithContext.KeyedCombineFnWithContext<? super Object, InputT, AccumT, OutputT> combineFn) {
        throw new UnsupportedOperationException("Here");
      }

      @Override
      public <W extends BoundedWindow> WatermarkHoldState<W> bindWatermark(StateTag<? super Object, WatermarkHoldState<W>> address, OutputTimeFn<? super W> outputTimeFn) {
        return new WatermarkHoldState<W>() {
          @Override
          public OutputTimeFn<? super W> getOutputTimeFn() {
            return OutputTimeFns.outputAtEndOfWindow();
          }

          @Override
          public WatermarkHoldState<W> readLater() {
            return this;
          }

          @Override
          public void add(Instant value) {

          }

          @Override
          public ReadableState<Boolean> isEmpty() {
            return new ReadableState<Boolean>() {
              @Override
              public Boolean read() {
                return true;
              }

              @Override
              public ReadableState<Boolean> readLater() {
                return this;
              }
            };
          }

          @Override
          public Instant read() {
            return new Instant(Long.MAX_VALUE);
          }

          @Override
          public void clear() {

          }
        };
      }
    });
  }
}
