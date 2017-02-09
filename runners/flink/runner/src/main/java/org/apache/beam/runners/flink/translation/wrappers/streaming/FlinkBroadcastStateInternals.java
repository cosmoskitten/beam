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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.util.CombineContextFactory;
import org.apache.beam.sdk.util.state.AccumulatorCombiningState;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.util.state.ReadableState;
import org.apache.beam.sdk.util.state.State;
import org.apache.beam.sdk.util.state.StateContext;
import org.apache.beam.sdk.util.state.StateContexts;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.util.state.WatermarkHoldState;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;

/**
 * {@link StateInternals} that uses a Flink {@link DefaultOperatorStateBackend}
 * to manage state.
 * Ignore key. Mainly for SideInputs.
 */
public class FlinkBroadcastStateInternals<K> implements StateInternals<K> {

  private final DefaultOperatorStateBackend stateBackend;

  public FlinkBroadcastStateInternals(OperatorStateBackend stateBackend) {
    //TODO flink do not yet expose through public API
    this.stateBackend = (DefaultOperatorStateBackend) stateBackend;
  }

  @Override
  public K getKey() {
    return null;
  }

  @Override
  public <T extends State> T state(
      final StateNamespace namespace,
      StateTag<? super K, T> address) {

    return state(namespace, address, StateContexts.nullContext());
  }

  @Override
  public <T extends State> T state(
      final StateNamespace namespace,
      StateTag<? super K, T> address,
      final StateContext<?> context) {

    return address.bind(new StateTag.StateBinder<K>() {

      @Override
      public <T> ValueState<T> bindValue(
          StateTag<? super K, ValueState<T>> address,
          Coder<T> coder) {

        return new FlinkValueState<>(stateBackend, address, namespace, coder);
      }

      @Override
      public <T> BagState<T> bindBag(
          StateTag<? super K, BagState<T>> address,
          Coder<T> elemCoder) {

        return new FlinkBagState<>(stateBackend, address, namespace, elemCoder);
      }

      @Override
      public <InputT, AccumT, OutputT>
          AccumulatorCombiningState<InputT, AccumT, OutputT>
      bindCombiningValue(
          StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
          Coder<AccumT> accumCoder,
          Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {

        return new FlinkAccumulatorCombiningState<>(
            stateBackend, address, combineFn, namespace, accumCoder);
      }

      @Override
      public <InputT, AccumT, OutputT>
          AccumulatorCombiningState<InputT, AccumT, OutputT> bindKeyedCombiningValue(
          StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
          Coder<AccumT> accumCoder,
          final Combine.KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn) {
        return new FlinkKeyedAccumulatorCombiningState<>(
            stateBackend,
            address,
            combineFn,
            namespace,
            accumCoder,
            FlinkBroadcastStateInternals.this);
      }

      @Override
      public <InputT, AccumT, OutputT>
          AccumulatorCombiningState<InputT, AccumT, OutputT> bindKeyedCombiningValueWithContext(
          StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
          Coder<AccumT> accumCoder,
          CombineWithContext.KeyedCombineFnWithContext<
              ? super K, InputT, AccumT, OutputT> combineFn) {
        return new FlinkAccumulatorCombiningStateWithContext<>(
            stateBackend,
            address,
            combineFn,
            namespace,
            accumCoder,
            FlinkBroadcastStateInternals.this,
            CombineContextFactory.createFromStateContext(context));
      }

      @Override
      public <W extends BoundedWindow> WatermarkHoldState<W> bindWatermark(
          StateTag<? super K, WatermarkHoldState<W>> address,
          OutputTimeFn<? super W> outputTimeFn) {
         throw new UnsupportedOperationException("bindWatermark is not supported.");
      }
    });
  }

  private static class BroadcastValue<T> {

    private final StateNamespace namespace;
    private final ListStateDescriptor<Map<String, T>> flinkStateDescriptor;
    private final DefaultOperatorStateBackend flinkStateBackend;

    BroadcastValue(
        DefaultOperatorStateBackend flinkStateBackend,
        String name,
        StateNamespace namespace,
        Coder<T> coder) {

      this.namespace = namespace;
      this.flinkStateBackend = flinkStateBackend;

      CoderTypeInformation<Map<String, T>> typeInfo =
          new CoderTypeInformation<>(MapCoder.of(StringUtf8Coder.of(), coder));

      flinkStateDescriptor = new ListStateDescriptor<>(name,
          typeInfo.createSerializer(new ExecutionConfig()));
    }

    Map<String, T> getMap() throws Exception {
      ListState<Map<String, T>> state = flinkStateBackend.getBroadcastOperatorState(
          flinkStateDescriptor);
      Iterable<Map<String, T>> iterable = state.get();
      Map<String, T> ret = null;
      if (iterable != null) {
        Iterator<Map<String, T>> iterator = iterable.iterator();
        if (iterator.hasNext()) {
          ret = iterator.next();
        }
      }
      return ret;
    }

    void putMap(Map<String, T> map) throws Exception {
      ListState<Map<String, T>> state = flinkStateBackend.getBroadcastOperatorState(
          flinkStateDescriptor);
      state.clear();
      if (map.size() > 0) {
        state.add(map);
      }
    }

    void writeInternal(T input) {
      try {
        Map<String, T> map = getMap();
        if (map == null) {
          map = new HashMap<>();
        }
        map.put(namespace.stringKey(), input);
        putMap(map);
      } catch (Exception e) {
        throw new RuntimeException("Error updating state.", e);
      }
    }

    T readInternal() {
      try {
        Map<String, T> map = getMap();
        if (map == null) {
          return null;
        } else {
          return map.get(namespace.stringKey());
        }
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    void clearInternal() {
      try {
        Map<String, T> map = getMap();
        if (map != null) {
          map.remove(namespace.stringKey());
          putMap(map);
        }
      } catch (Exception e) {
        throw new RuntimeException("Error clearing state.", e);
      }
    }

  }

  private static class FlinkValueState<K, T> extends BroadcastValue<T> implements ValueState<T> {

    private final StateNamespace namespace;
    private final StateTag<? super K, ValueState<T>> address;

    FlinkValueState(
        DefaultOperatorStateBackend flinkStateBackend,
        StateTag<? super K, ValueState<T>> address,
        StateNamespace namespace,
        Coder<T> coder) {
      super(flinkStateBackend, address.getId(), namespace, coder);

      this.namespace = namespace;
      this.address = address;

    }

    @Override
    public void write(T input) {
      writeInternal(input);
    }

    @Override
    public ValueState<T> readLater() {
      return this;
    }

    @Override
    public T read() {
      return readInternal();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkValueState<?, ?> that = (FlinkValueState<?, ?>) o;

      return namespace.equals(that.namespace) && address.equals(that.address);

    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }

    @Override
    public void clear() {
      clearInternal();
    }
  }

  private static class FlinkBagState<K, T> extends BroadcastValue<List<T>> implements BagState<T> {

    private final StateNamespace namespace;
    private final StateTag<? super K, BagState<T>> address;

    FlinkBagState(
        DefaultOperatorStateBackend flinkStateBackend,
        StateTag<? super K, BagState<T>> address,
        StateNamespace namespace,
        Coder<T> coder) {
      super(flinkStateBackend, address.getId(), namespace, ListCoder.of(coder));

      this.namespace = namespace;
      this.address = address;
    }

    @Override
    public void add(T input) {
      List<T> list = readInternal();
      if (list == null) {
        list = new ArrayList<>();
      }
      list.add(input);
      writeInternal(list);
    }

    @Override
    public BagState<T> readLater() {
      return this;
    }

    @Override
    public Iterable<T> read() {
      List<T> result = readInternal();
      return result != null ? result : Collections.<T>emptyList();
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          try {
            List<T> result = readInternal();
            return result == null;
          } catch (Exception e) {
            throw new RuntimeException("Error reading state.", e);
          }

        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public void clear() {
      clearInternal();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkBagState<?, ?> that = (FlinkBagState<?, ?>) o;

      return namespace.equals(that.namespace) && address.equals(that.address);

    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }
  }

  private static class FlinkAccumulatorCombiningState<K, InputT, AccumT, OutputT>
      extends BroadcastValue<AccumT> implements AccumulatorCombiningState<InputT, AccumT, OutputT> {

    private final StateNamespace namespace;
    private final StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address;
    private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;

    FlinkAccumulatorCombiningState(
        DefaultOperatorStateBackend flinkStateBackend,
        StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
        Combine.CombineFn<InputT, AccumT, OutputT> combineFn,
        StateNamespace namespace,
        Coder<AccumT> accumCoder) {
      super(flinkStateBackend, address.getId(), namespace, accumCoder);

      this.namespace = namespace;
      this.address = address;
      this.combineFn = combineFn;
    }

    @Override
    public AccumulatorCombiningState<InputT, AccumT, OutputT> readLater() {
      return this;
    }

    @Override
    public void add(InputT value) {
      AccumT current = readInternal();
      if (current == null) {
        current = combineFn.createAccumulator();
      }
      current = combineFn.addInput(current, value);
      writeInternal(current);
    }

    @Override
    public void addAccum(AccumT accum) {
      AccumT current = readInternal();

      if (current == null) {
        writeInternal(accum);
      } else {
        current = combineFn.mergeAccumulators(Arrays.asList(current, accum));
        writeInternal(current);
      }
    }

    @Override
    public AccumT getAccum() {
      return readInternal();
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(accumulators);
    }

    @Override
    public OutputT read() {
      AccumT accum = readInternal();
      if (accum != null) {
        return combineFn.extractOutput(accum);
      } else {
        return combineFn.extractOutput(combineFn.createAccumulator());
      }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          try {
            return readInternal() == null;
          } catch (Exception e) {
            throw new RuntimeException("Error reading state.", e);
          }

        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public void clear() {
      clearInternal();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkAccumulatorCombiningState<?, ?, ?, ?> that =
          (FlinkAccumulatorCombiningState<?, ?, ?, ?>) o;

      return namespace.equals(that.namespace) && address.equals(that.address);

    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }
  }

  private static class FlinkKeyedAccumulatorCombiningState<K, InputT, AccumT, OutputT>
      extends BroadcastValue<AccumT> implements AccumulatorCombiningState<InputT, AccumT, OutputT> {

    private final StateNamespace namespace;
    private final StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address;
    private final Combine.KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn;
    private final FlinkBroadcastStateInternals<K> flinkStateInternals;

    FlinkKeyedAccumulatorCombiningState(
        DefaultOperatorStateBackend flinkStateBackend,
        StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
        Combine.KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn,
        StateNamespace namespace,
        Coder<AccumT> accumCoder,
        FlinkBroadcastStateInternals<K> flinkStateInternals) {
      super(flinkStateBackend, address.getId(), namespace, accumCoder);

      this.namespace = namespace;
      this.address = address;
      this.combineFn = combineFn;
      this.flinkStateInternals = flinkStateInternals;

    }

    @Override
    public AccumulatorCombiningState<InputT, AccumT, OutputT> readLater() {
      return this;
    }

    @Override
    public void add(InputT value) {
      try {
        AccumT current = readInternal();
        if (current == null) {
          current = combineFn.createAccumulator(flinkStateInternals.getKey());
        }
        current = combineFn.addInput(flinkStateInternals.getKey(), current, value);
        writeInternal(current);
      } catch (Exception e) {
        throw new RuntimeException("Error adding to state." , e);
      }
    }

    @Override
    public void addAccum(AccumT accum) {
      try {
        AccumT current = readInternal();
        if (current == null) {
          writeInternal(accum);
        } else {
          current = combineFn.mergeAccumulators(
              flinkStateInternals.getKey(),
              Arrays.asList(current, accum));
          writeInternal(current);
        }
      } catch (Exception e) {
        throw new RuntimeException("Error adding to state.", e);
      }
    }

    @Override
    public AccumT getAccum() {
      try {
        return readInternal();
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(flinkStateInternals.getKey(), accumulators);
    }

    @Override
    public OutputT read() {
      try {
        AccumT accum = readInternal();
        if (accum != null) {
          return combineFn.extractOutput(flinkStateInternals.getKey(), accum);
        } else {
          return combineFn.extractOutput(
              flinkStateInternals.getKey(),
              combineFn.createAccumulator(flinkStateInternals.getKey()));
        }
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          try {
            return readInternal() == null;
          } catch (Exception e) {
            throw new RuntimeException("Error reading state.", e);
          }

        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public void clear() {
      clearInternal();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkKeyedAccumulatorCombiningState<?, ?, ?, ?> that =
          (FlinkKeyedAccumulatorCombiningState<?, ?, ?, ?>) o;

      return namespace.equals(that.namespace) && address.equals(that.address);

    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }
  }

  private static class FlinkAccumulatorCombiningStateWithContext<K, InputT, AccumT, OutputT>
      extends BroadcastValue<AccumT> implements AccumulatorCombiningState<InputT, AccumT, OutputT> {

    private final StateNamespace namespace;
    private final StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address;
    private final CombineWithContext.KeyedCombineFnWithContext<
        ? super K, InputT, AccumT, OutputT> combineFn;
    private final FlinkBroadcastStateInternals<K> flinkStateInternals;
    private final CombineWithContext.Context context;

    FlinkAccumulatorCombiningStateWithContext(
        DefaultOperatorStateBackend flinkStateBackend,
        StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
        CombineWithContext.KeyedCombineFnWithContext<
            ? super K, InputT, AccumT, OutputT> combineFn,
        StateNamespace namespace,
        Coder<AccumT> accumCoder,
        FlinkBroadcastStateInternals<K> flinkStateInternals,
        CombineWithContext.Context context) {
      super(flinkStateBackend, address.getId(), namespace, accumCoder);

      this.namespace = namespace;
      this.address = address;
      this.combineFn = combineFn;
      this.flinkStateInternals = flinkStateInternals;
      this.context = context;

    }

    @Override
    public AccumulatorCombiningState<InputT, AccumT, OutputT> readLater() {
      return this;
    }

    @Override
    public void add(InputT value) {
      try {
        AccumT current = readInternal();
        if (current == null) {
          current = combineFn.createAccumulator(flinkStateInternals.getKey(), context);
        }
        current = combineFn.addInput(flinkStateInternals.getKey(), current, value, context);
        writeInternal(current);
      } catch (Exception e) {
        throw new RuntimeException("Error adding to state." , e);
      }
    }

    @Override
    public void addAccum(AccumT accum) {
      try {

        AccumT current = readInternal();
        if (current == null) {
          writeInternal(accum);
        } else {
          current = combineFn.mergeAccumulators(
              flinkStateInternals.getKey(),
              Arrays.asList(current, accum),
              context);
          writeInternal(current);
        }
      } catch (Exception e) {
        throw new RuntimeException("Error adding to state.", e);
      }
    }

    @Override
    public AccumT getAccum() {
      try {
        return readInternal();
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(flinkStateInternals.getKey(), accumulators, context);
    }

    @Override
    public OutputT read() {
      try {
        AccumT accum = readInternal();
        return combineFn.extractOutput(flinkStateInternals.getKey(), accum, context);
      } catch (Exception e) {
        throw new RuntimeException("Error reading state.", e);
      }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          try {
            return readInternal() == null;
          } catch (Exception e) {
            throw new RuntimeException("Error reading state.", e);
          }

        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public void clear() {
      clearInternal();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkAccumulatorCombiningStateWithContext<?, ?, ?, ?> that =
          (FlinkAccumulatorCombiningStateWithContext<?, ?, ?, ?>) o;

      return namespace.equals(that.namespace) && address.equals(that.address);

    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }
  }

}
