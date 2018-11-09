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

import static org.apache.flink.util.Preconditions.checkNotNull;

import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.TimerReference;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageContext;
import org.apache.beam.runners.flink.translation.functions.FlinkStreamingSideInputHandlerFactory;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This operator is the streaming equivalent of the {@link
 * org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageFunction}. It sends all
 * received elements to the SDK harness and emits the received back elements to the downstream
 * operators. It also takes care of handling side inputs and state.
 *
 * <p>TODO Integrate support for progress updates and metrics
 */
public class ExecutableStageDoFnOperator<InputT, OutputT> extends DoFnOperator<InputT, OutputT> {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutableStageDoFnOperator.class);

  private final RunnerApi.ExecutableStagePayload payload;
  private final JobInfo jobInfo;
  private final FlinkExecutableStageContext.Factory contextFactory;
  private final Map<String, TupleTag<?>> outputMap;
  private final Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInputIds;
  private final boolean stateful;
  private final boolean usesTimers;

  private transient FlinkExecutableStageContext stageContext;
  private transient StateRequestHandler stateRequestHandler;
  private transient BundleProgressHandler progressHandler;
  private transient StageBundleFactory stageBundleFactory;
  private transient ExecutableStage executableStage;
  private transient SdkHarnessDoFnRunner<InputT, OutputT> sdkHarnessRunner;

  public ExecutableStageDoFnOperator(
      String stepName,
      Coder<WindowedValue<InputT>> windowedInputCoder,
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      OutputManagerFactory<OutputT> outputManagerFactory,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>> sideInputIds,
      PipelineOptions options,
      RunnerApi.ExecutableStagePayload payload,
      JobInfo jobInfo,
      FlinkExecutableStageContext.Factory contextFactory,
      Map<String, TupleTag<?>> outputMap,
      Coder keyCoder,
      KeySelector<WindowedValue<InputT>, ?> keySelector) {
    super(
        new NoOpDoFn(),
        stepName,
        windowedInputCoder,
        inputCoder,
        outputCoders,
        mainOutputTag,
        additionalOutputTags,
        outputManagerFactory,
        WindowingStrategy.globalDefault() /* unused */,
        sideInputTagMapping,
        sideInputs,
        options,
        keyCoder,
        keySelector);
    this.payload = payload;
    this.jobInfo = jobInfo;
    this.contextFactory = contextFactory;
    this.outputMap = outputMap;
    this.sideInputIds = sideInputIds;
    this.stateful = payload.getUserStatesCount() > 0;
    this.usesTimers = payload.getTimersCount() > 0;
  }

  @Override
  public void open() throws Exception {
    executableStage = ExecutableStage.fromPayload(payload);
    // TODO: Wire this into the distributed cache and make it pluggable.
    // TODO: Do we really want this layer of indirection when accessing the stage bundle factory?
    // It's a little strange because this operator is responsible for the lifetime of the stage
    // bundle "factory" (manager?) but not the job or Flink bundle factories. How do we make
    // ownership of the higher level "factories" explicit? Do we care?
    stageContext = contextFactory.get(jobInfo);

    stageBundleFactory = stageContext.getStageBundleFactory(executableStage);
    stateRequestHandler = getStateRequestHandler(executableStage);
    progressHandler = BundleProgressHandler.unsupported();

    super.open();
  }

  private StateRequestHandler getStateRequestHandler(ExecutableStage executableStage) {

    final StateRequestHandler sideInputStateHandler;
    if (executableStage.getSideInputs().size() > 0) {
      checkNotNull(super.sideInputHandler);
      StateRequestHandlers.SideInputHandlerFactory sideInputHandlerFactory =
          Preconditions.checkNotNull(
              FlinkStreamingSideInputHandlerFactory.forStage(
                  executableStage, sideInputIds, super.sideInputHandler));
      try {
        sideInputStateHandler =
            StateRequestHandlers.forSideInputHandlerFactory(
                ProcessBundleDescriptors.getSideInputs(executableStage), sideInputHandlerFactory);
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize SideInputHandler", e);
      }
    } else {
      sideInputStateHandler = StateRequestHandler.unsupported();
    }

    final StateRequestHandler userStateRequestHandler;
    if (executableStage.getUserStates().size() > 0) {
      if (keyedStateInternals == null) {
        throw new IllegalStateException("Input must be keyed when user state is used");
      }
      userStateRequestHandler =
          StateRequestHandlers.forBagUserStateHandlerFactory(
              stageBundleFactory.getProcessBundleDescriptor(),
              new BagUserStateFactory(keyedStateInternals, getKeyedStateBackend()));
    } else {
      userStateRequestHandler = StateRequestHandler.unsupported();
    }

    EnumMap<TypeCase, StateRequestHandler> handlerMap = new EnumMap<>(TypeCase.class);
    handlerMap.put(TypeCase.MULTIMAP_SIDE_INPUT, sideInputStateHandler);
    handlerMap.put(TypeCase.BAG_USER_STATE, userStateRequestHandler);

    return StateRequestHandlers.delegateBasedUponType(handlerMap);
  }

  private static class BagUserStateFactory
      implements StateRequestHandlers.BagUserStateHandlerFactory {

    private final StateInternals stateInternals;
    private final KeyedStateBackend<ByteBuffer> keyedStateBackend;

    private BagUserStateFactory(
        StateInternals stateInternals, KeyedStateBackend<ByteBuffer> keyedStateBackend) {

      this.stateInternals = stateInternals;
      this.keyedStateBackend = keyedStateBackend;
    }

    @Override
    public <K, V, W extends BoundedWindow>
        StateRequestHandlers.BagUserStateHandler<K, V, W> forUserState(
            String pTransformId,
            String userStateId,
            Coder<K> keyCoder,
            Coder<V> valueCoder,
            Coder<W> windowCoder) {
      return new StateRequestHandlers.BagUserStateHandler<K, V, W>() {
        @Override
        public Iterable<V> get(K key, W window) {
          synchronized (keyedStateBackend) {
            // key needs to be set explicitly and synchronized to work with the asynchronous
            // processing of the SDK harness
            prepareStateBackend(key, keyCoder);
            StateNamespace namespace = StateNamespaces.window(windowCoder, window);
            BagState<V> bagState =
                stateInternals.state(namespace, StateTags.bag(userStateId, valueCoder));
            return bagState.read();
          }
        }

        @Override
        public void append(K key, W window, Iterator<V> values) {
          synchronized (keyedStateBackend) {
            // key needs to be set explicitly and synchronized to work with the asynchronous
            // processing of the SDK harness
            prepareStateBackend(key, keyCoder);
            StateNamespace namespace = StateNamespaces.window(windowCoder, window);
            BagState<V> bagState =
                stateInternals.state(namespace, StateTags.bag(userStateId, valueCoder));
            while (values.hasNext()) {
              bagState.add(values.next());
            }
          }
        }

        @Override
        public void clear(K key, W window) {
          synchronized (keyedStateBackend) {
            // key needs to be set explicitly and synchronized to work with the asynchronous
            // processing of the SDK harness
            prepareStateBackend(key, keyCoder);
            StateNamespace namespace = StateNamespaces.window(windowCoder, window);
            BagState<V> bagState =
                stateInternals.state(namespace, StateTags.bag(userStateId, valueCoder));
            bagState.clear();
          }
        }

        private void prepareStateBackend(K key, Coder<K> keyCoder) {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          try {
            keyCoder.encode(key, baos);
          } catch (IOException e) {
            throw new RuntimeException("Failed to encode key for Flink state backend", e);
          }
          keyedStateBackend.setCurrentKey(ByteBuffer.wrap(baos.toByteArray()));
        }
      };
    }
  }

  @Override
  public void setKeyContextElement1(StreamRecord record) throws Exception {
    // Note: This is only relevant when we have a stateful DoFn.
    // We want to control the key of the state backend ourselves and
    // we must avoid any concurrent setting of the current active key.
    // By overwriting this, we also prevent unnecessary serialization
    // as the key has to be encoded as a byte array.
  }

  @Override
  public void setCurrentKey(Object key) {
    if (stateful && usesTimers) {
      // Current key has to be set for state access and timers processing.
      // We need to synchronize because both can happen concurrently, i.e.
      // state access from the SDK harness and timer processing in the
      // operator.
      synchronized (getKeyedStateBackend()) {
        super.setCurrentKey(key);
      }
    } else if (usesTimers) {
      // Set by Flink's HeapInternalTimerService before firing a timer
      // and when setting a timer for the aforementioned.
      super.setCurrentKey(key);
    } else {
      throw new UnsupportedOperationException(
          "Current key for state backend can only be set by state requests from SDK workers or when processing timers.");
    }
  }

  @Override
  public void dispose() throws Exception {
    // may be called multiple times when an exception is thrown
    if (stageContext != null) {
      // Remove the reference to stageContext and make stageContext available for garbage collection.
      try (@SuppressWarnings("unused")
              AutoCloseable bundleFactoryCloser = stageBundleFactory;
          @SuppressWarnings("unused")
              AutoCloseable closable = stageContext) {
        // DoFnOperator generates another "bundle" for the final watermark
        // https://issues.apache.org/jira/browse/BEAM-5816
        super.dispose();
      } finally {
        stageContext = null;
      }
    }
  }

  @Override
  protected void addSideInputValue(StreamRecord<RawUnionValue> streamRecord) {
    @SuppressWarnings("unchecked")
    WindowedValue<KV<Void, Iterable<?>>> value =
        (WindowedValue<KV<Void, Iterable<?>>>) streamRecord.getValue().getValue();
    PCollectionView<?> sideInput = sideInputTagMapping.get(streamRecord.getValue().getUnionTag());
    sideInputHandler.addSideInputValue(sideInput, value.withValue(value.getValue().getValue()));
  }

  @Override
  protected DoFnRunner<InputT, OutputT> createWrappingDoFnRunner(
      DoFnRunner<InputT, OutputT> wrappedRunner) {
    sdkHarnessRunner =
        new SdkHarnessDoFnRunner<>(
            executableStage.getInputPCollection().getId(),
            stageBundleFactory,
            stateRequestHandler,
            progressHandler,
            outputManager,
            outputMap,
            executableStage.getTimers(),
            (Coder<BoundedWindow>) windowingStrategy.getWindowFn().windowCoder(),
            (WindowedValue<InputT> key, TimerInternals.TimerData timerData) -> {
              try {
                synchronized (getKeyedStateBackend()) {
                  setCurrentKey(keySelector.getKey(key));
                  timerInternals.setTimer(timerData);
                }
              } catch (Exception e) {
                throw new RuntimeException("Couldn't set key", e);
              }
            },
            () -> {
              synchronized (getKeyedStateBackend()) {
                ByteBuffer encodedKey = (ByteBuffer) getKeyedStateBackend().getCurrentKey();
                @SuppressWarnings("ByteBufferBackingArray")
                ByteArrayInputStream byteStream = new ByteArrayInputStream(encodedKey.array());
                try {
                  return keyCoder.decode(byteStream);
                } catch (IOException e) {
                  throw new RuntimeException(
                      String.format(
                          Locale.ENGLISH, "Failed to decode encoded key: %s", encodedKey));
                }
              }
            });
    return sdkHarnessRunner;
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    // Due to the asynchronous communication with the SDK harness,
    // a bundle might still be in progress and not all items have
    // yet been received from the SDk harness. If we just set this
    // watermark as the new output watermark, we could violate the
    // order of the records, i.e. pending items in the SDK harness
    // could become "late" although they were "on time".
    //
    // We can solve this problem using one of the following options:
    //
    // 1) Finish the current bundle and emit this watermark as the
    //    new output watermark. Finishing the bundle ensures that
    //    all the items have been processed by the SDK harness and
    //    received by the outputQueue (see below), where they will
    //    have been emitted to the output stream.
    //
    // 2) Put a hold on the output watermark for as long as the current
    //    bundle has not been finished. We have to remember to manually
    //    finish the bundle in case we receive the final watermark.
    //    To avoid latency, we should process this watermark again as
    //    soon as the current bundle is finished.
    //
    // Approach 1) is the easiest, yet 2) gives better throughput due
    // to the bundle getting cut on every watermark. So we have
    // implemented 2) below.
    //
    if (sdkHarnessRunner.isBundleInProgress()) {
      if (mark.getTimestamp() >= BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
        invokeFinishBundle();
      } else {
        // It is not safe to advance the output watermark yet, so add a hold on the current
        // output watermark.
        setPushedBackWatermark(Math.min(currentOutputWatermark, getPushbackWatermarkHold()));
        sdkHarnessRunner.setBundleFinishedCallback(
            () -> {
              try {
                processWatermark(mark);
              } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to process pushed back watermark after finished bundle.", e);
              }
            });
      }
    }
    super.processWatermark(mark);
  }

  private static class SdkHarnessDoFnRunner<InputT, OutputT>
      implements DoFnRunner<InputT, OutputT> {

    private final String mainInput;
    private final LinkedBlockingQueue<KV<String, OutputT>> outputQueue;
    private final StageBundleFactory stageBundleFactory;
    private final StateRequestHandler stateRequestHandler;
    private final BundleProgressHandler progressHandler;
    private final BufferedOutputManager<OutputT> outputManager;
    private final Map<String, TupleTag<?>> outputMap;
    /** Timer PCollection id => TimerReference. */
    private final Map<String, TimerReference> timerReferenceMap;

    private final Coder<BoundedWindow> windowCoder;
    private final BiConsumer<WindowedValue<InputT>, TimerInternals.TimerData> timerDataConsumer;
    private final Supplier<Object> currentKeySupplier;

    private RemoteBundle remoteBundle;
    private FnDataReceiver<WindowedValue<?>> mainInputReceiver;
    private Runnable bundleFinishedCallback;

    public SdkHarnessDoFnRunner(
        String mainInput,
        StageBundleFactory stageBundleFactory,
        StateRequestHandler stateRequestHandler,
        BundleProgressHandler progressHandler,
        BufferedOutputManager<OutputT> outputManager,
        Map<String, TupleTag<?>> outputMap,
        Collection<TimerReference> timers,
        Coder<BoundedWindow> windowCoder,
        BiConsumer<WindowedValue<InputT>, TimerInternals.TimerData> timerDataConsumer,
        Supplier<Object> currentKeySupplier) {
      this.mainInput = mainInput;
      this.stageBundleFactory = stageBundleFactory;
      this.stateRequestHandler = stateRequestHandler;
      this.progressHandler = progressHandler;
      this.outputManager = outputManager;
      this.outputMap = outputMap;
      this.currentKeySupplier = currentKeySupplier;
      this.timerReferenceMap = new HashMap<>();
      for (TimerReference timer : timers) {
        timerReferenceMap.put(timer.collection().getId(), timer);
      }
      this.windowCoder = windowCoder;
      this.timerDataConsumer = timerDataConsumer;
      this.outputQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void startBundle() {
      OutputReceiverFactory receiverFactory =
          new OutputReceiverFactory() {
            @Override
            public FnDataReceiver<OutputT> create(String pCollectionId) {
              return receivedElement -> {
                // handover to queue, do not block the grpc thread
                outputQueue.put(KV.of(pCollectionId, receivedElement));
              };
            }
          };

      try {
        remoteBundle =
            stageBundleFactory.getBundle(receiverFactory, stateRequestHandler, progressHandler);
        mainInputReceiver =
            Preconditions.checkNotNull(
                remoteBundle.getInputReceivers().get(mainInput),
                "Failed to retrieve main input receiver.");
      } catch (Exception e) {
        throw new RuntimeException("Failed to start remote bundle", e);
      }
    }

    @Override
    public void processElement(WindowedValue<InputT> element) {
      try {
        LOG.debug("Sending value: {}", element);
        mainInputReceiver.accept(element);
      } catch (Exception e) {
        throw new RuntimeException("Failed to process element with SDK harness.", e);
      }
      emitResults();
    }

    @Override
    public void onTimer(
        String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain) {
      LOG.debug("timer callback: {} {} {} {}", timerId, window, timestamp, timeDomain);
      FnDataReceiver<WindowedValue<?>> timerReceiver =
          Preconditions.checkNotNull(
              remoteBundle.getInputReceivers().get(timerId),
              "No receiver found for timer %s",
              timerId);
      WindowedValue<KV<Object, Timer>> timerValue =
          WindowedValue.of(
              KV.of(currentKeySupplier.get(), Timer.of(timestamp, new byte[0])),
              timestamp,
              Collections.singleton(window),
              PaneInfo.NO_FIRING);
      try {
        timerReceiver.accept(timerValue);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format(Locale.ENGLISH, "Failed to process timer %s", timerReceiver), e);
      }
    }

    @Override
    public void finishBundle() {
      try {
        // TODO: it would be nice to emit results as they arrive, can thread wait non-blocking?
        // close blocks until all results are received
        remoteBundle.close();
        emitResults();
      } catch (Exception e) {
        throw new RuntimeException("Failed to finish remote bundle", e);
      } finally {
        remoteBundle = null;
      }
      if (bundleFinishedCallback != null) {
        bundleFinishedCallback.run();
        bundleFinishedCallback = null;
      }
    }

    boolean isBundleInProgress() {
      return remoteBundle != null;
    }

    void setBundleFinishedCallback(Runnable callback) {
      this.bundleFinishedCallback = callback;
    }

    private void emitResults() {
      KV<String, OutputT> result;
      while ((result = outputQueue.poll()) != null) {
        final String inputCollectionId = result.getKey();
        TupleTag<?> tag = outputMap.get(inputCollectionId);
        WindowedValue windowedValue =
            Preconditions.checkNotNull(
                (WindowedValue) result.getValue(),
                "Received a null value from the SDK harness for %s",
                inputCollectionId);
        if (tag != null) {
          // process regular elements
          outputManager.output(tag, windowedValue);
        } else {
          // process timer elements
          // TODO This is ugly. There should be an easier way to retrieve the
          String timerPCollectionId =
              inputCollectionId.substring(0, inputCollectionId.length() - ".out:0".length());
          TimerReference timerReference = timerReferenceMap.get(timerPCollectionId);
          if (timerReference != null) {
            Timer timer =
                Preconditions.checkNotNull(
                    (Timer) ((KV) windowedValue.getValue()).getValue(),
                    "Received null Timer from SDK harness: %s",
                    windowedValue);
            LOG.debug("Timer received: {} {}", inputCollectionId, timer);
            for (Object window : windowedValue.getWindows()) {
              StateNamespace namespace =
                  StateNamespaces.window(windowCoder, (BoundedWindow) window);
              TimerSpec timerSpec = extractTimerSpec(timerReference);
              TimerInternals.TimerData timerData =
                  TimerInternals.TimerData.of(
                      timerPCollectionId,
                      namespace,
                      timer.getTimestamp(),
                      timerSpec.getTimeDomain());
              timerDataConsumer.accept(windowedValue, timerData);
            }
          } else {
            throw new IllegalStateException(
                String.format(Locale.ENGLISH, "Unknown PCollectionId %s", inputCollectionId));
          }
        }
      }
    }

    private TimerSpec extractTimerSpec(TimerReference timerReference) {
      String transformId = timerReference.transform().getId();
      Map<String, ProcessBundleDescriptors.TimerSpec> stringTimerSpecMap =
          Preconditions.checkNotNull(
              stageBundleFactory.getProcessBundleDescriptor().getTimerSpecs().get(transformId),
              "No TimerSpec found in transform %s",
              transformId);
      String timerName = timerReference.localName();
      return Preconditions.checkNotNull(
              stringTimerSpecMap.get(timerName), "No TimerSpec found for timer %s", timerName)
          .getTimerSpec();
    }

    @Override
    public DoFn<InputT, OutputT> getFn() {
      throw new UnsupportedOperationException();
    }
  }

  private static class NoOpDoFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
    @ProcessElement
    public void doNothing(ProcessContext context) {}
  }
}
