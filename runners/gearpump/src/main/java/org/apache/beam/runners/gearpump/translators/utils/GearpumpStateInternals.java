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
package org.apache.beam.runners.gearpump.translators.utils;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.util.state.AccumulatorCombiningState;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.util.state.ReadableState;
import org.apache.beam.sdk.util.state.State;
import org.apache.beam.sdk.util.state.StateContext;
import org.apache.beam.sdk.util.state.StateContexts;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.util.state.StateNamespace;
import org.apache.beam.sdk.util.state.StateTag;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.util.state.WatermarkHoldState;
import org.joda.time.Instant;

/**
 * Gearpump StateInternals.
 */
public class GearpumpStateInternals<K> implements StateInternals<K>, Serializable {

  private static final long serialVersionUID = -425161366124461866L;
  public static <K> GearpumpStateInternals<K> forKey(K key) {
    return new GearpumpStateInternals<>(key);
  }

  private final K key;

  protected GearpumpStateInternals(K key) {
    this.key = key;
  }

  @Override
  public K getKey() {
    return key;
  }

  /**
   * Serializable state for internals (namespace to state tag to coded value).
   */
  private final Table<String, String, byte[]> stateTable = HashBasedTable.create();

  @Override
  public <T extends State> T state(StateNamespace namespace, StateTag<? super K, T> address) {
    return state(namespace, address, StateContexts.nullContext());
  }

  @Override
  public <T extends State> T state(
      StateNamespace namespace, StateTag<? super K, T> address, final StateContext<?> c) {
    return address.bind(new GearpumpStateBinder(key, namespace, address, c));
  }

  /**
   * A {@link StateTag.StateBinder} that returns {@link State} wrappers for serialized state.
   */
  private class GearpumpStateBinder implements StateTag.StateBinder<K> {
    private final K key;
    private final StateNamespace namespace;
    private final StateContext<?> c;

    private GearpumpStateBinder(K key, StateNamespace namespace, StateTag<? super K, ?> address,
                            StateContext<?> c) {
      this.key = key;
      this.namespace = namespace;
      this.c = c;
    }

    @Override
    public <T> ValueState<T> bindValue(
        StateTag<? super K, ValueState<T>> address, Coder<T> coder) {
      return new GearpumpValueState<>(namespace, address, coder);
    }

    @Override
    public <T> BagState<T> bindBag(
        final StateTag<? super K, BagState<T>> address, Coder<T> elemCoder) {
      return new GearpumpBagState<>(namespace, address, elemCoder);
    }

    @Override
    public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT>
    bindCombiningValue(
        StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
        Coder<AccumT> accumCoder,
        final Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
      return new GearpumpAccumulatorCombiningState<>(
          namespace,
          address,
          accumCoder,
          key,
          combineFn.<K>asKeyedFn()
      );
    }

    @Override
    public <W extends BoundedWindow> WatermarkHoldState<W> bindWatermark(
        StateTag<? super K, WatermarkHoldState<W>> address,
        OutputTimeFn<? super W> outputTimeFn) {
      return new GearpumpWatermarkHoldState<>(namespace, address, outputTimeFn);
    }

    @Override
    public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT>
    bindKeyedCombiningValue(
        StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
        Coder<AccumT> accumCoder,
        Combine.KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn) {
      return new GearpumpAccumulatorCombiningState<>(
          namespace,
          address,
          accumCoder,
          key, combineFn);
    }

    @Override
    public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT>
    bindKeyedCombiningValueWithContext(
        StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
        Coder<AccumT> accumCoder,
        CombineWithContext.KeyedCombineFnWithContext<? super K, InputT, AccumT, OutputT>
            combineFn) {
      return bindKeyedCombiningValue(address, accumCoder, CombineFnUtil.bindContext(combineFn, c));
    }
  }

  private class AbstractState<T> {
    protected final StateNamespace namespace;
    protected final StateTag<?, ? extends State> address;
    protected final Coder<T> coder;

    private AbstractState(
        StateNamespace namespace,
        StateTag<?, ? extends State> address,
        Coder<T> coder) {
      this.namespace = namespace;
      this.address = address;
      this.coder = coder;
    }

    protected T readValue() {
      T value = null;
      byte[] buf = stateTable.get(namespace.stringKey(), address.getId());
      if (buf != null) {
        // TODO: reuse input
        InputStream input = new ByteArrayInputStream(buf);
        try {
          return coder.decode(input, Coder.Context.OUTER);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return value;
    }

    public void writeValue(T input) {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      try {
        coder.encode(input, output, Coder.Context.OUTER);
        stateTable.put(namespace.stringKey(), address.getId(), output.toByteArray());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public void clear() {
      stateTable.remove(namespace.stringKey(), address.getId());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      @SuppressWarnings("unchecked")
      AbstractState<?> that = (AbstractState<?>) o;
      return namespace.equals(that.namespace) && address.equals(that.address);
    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }
  }

  private class GearpumpValueState<T> extends AbstractState<T> implements ValueState<T> {

    private GearpumpValueState(
        StateNamespace namespace,
        StateTag<?, ValueState<T>> address,
        Coder<T> coder) {
      super(namespace, address, coder);
    }

    @Override
    public GearpumpValueState<T> readLater() {
      return this;
    }

    @Override
    public T read() {
      return readValue();
    }

    @Override
    public void write(T input) {
      writeValue(input);
    }
  }

  private final class GearpumpWatermarkHoldState<W extends BoundedWindow>
      extends AbstractState<Instant> implements WatermarkHoldState<W> {

    private final OutputTimeFn<? super W> outputTimeFn;

    public GearpumpWatermarkHoldState(
        StateNamespace namespace,
        StateTag<?, WatermarkHoldState<W>> address,
        OutputTimeFn<? super W> outputTimeFn) {
      super(namespace, address, InstantCoder.of());
      this.outputTimeFn = outputTimeFn;
    }

    @Override
    public GearpumpWatermarkHoldState<W> readLater() {
      return this;
    }

    @Override
    public Instant read() {
      return readValue();
    }

    @Override
    public void add(Instant outputTime) {
      Instant combined = read();
      combined = (combined == null) ? outputTime : outputTimeFn.combine(combined, outputTime);
      writeValue(combined);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
        @Override
        public Boolean read() {
          return stateTable.get(namespace.stringKey(), address.getId()) == null;
        }
      };
    }

    @Override
    public OutputTimeFn<? super W> getOutputTimeFn() {
      return outputTimeFn;
    }

  }

  private final class GearpumpAccumulatorCombiningState<K, InputT, AccumT, OutputT>
      extends AbstractState<AccumT>
      implements AccumulatorCombiningState<InputT, AccumT, OutputT> {
    private final K key;
    private final Combine.KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn;

    private GearpumpAccumulatorCombiningState(StateNamespace namespace,
        StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
        Coder<AccumT> coder,
        K key, Combine.KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn) {
      super(namespace, address, coder);
      this.key = key;
      this.combineFn = combineFn;
    }

    @Override
    public GearpumpAccumulatorCombiningState<K, InputT, AccumT, OutputT> readLater() {
      return this;
    }

    @Override
    public OutputT read() {
      return combineFn.extractOutput(key, getAccum());
    }

    @Override
    public void add(InputT input) {
      AccumT accum = getAccum();
      combineFn.addInput(key, accum, input);
      writeValue(accum);
    }

    @Override
    public AccumT getAccum() {
      AccumT accum = readValue();
      if (accum == null) {
        accum = combineFn.createAccumulator(key);
      }
      return accum;
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
        @Override
        public Boolean read() {
          return stateTable.get(namespace.stringKey(), address.getId()) == null;
        }
      };
    }

    @Override
    public void addAccum(AccumT accum) {
      accum = combineFn.mergeAccumulators(key, Arrays.asList(getAccum(), accum));
      writeValue(accum);
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(key, accumulators);
    }

  }

  private final class GearpumpBagState<T> extends AbstractState<List<T>> implements BagState<T> {
    private GearpumpBagState(
        StateNamespace namespace,
        StateTag<?, BagState<T>> address,
        Coder<T> coder) {
      super(namespace, address, ListCoder.of(coder));
    }

    @Override
    public GearpumpBagState<T> readLater() {
      return this;
    }

    @Override
    public List<T> read() {
      List<T> value = super.readValue();
      if (value == null) {
        value = new ArrayList<>();
      }
      return value;
    }

    @Override
    public void add(T input) {
      List<T> value = read();
      value.add(input);
      writeValue(value);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }

        @Override
        public Boolean read() {
          return stateTable.get(namespace.stringKey(), address.getId()) == null;
        }
      };
    }
  }
}
