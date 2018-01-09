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

package org.apache.beam.runners.direct;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.DoFn.Teardown;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.SerializableUtils;

/**
 * Manages {@link DoFn} setup, teardown, and serialization.
 *
 * <p>{@link DoFnLifecycleManager} is similar to a {@link ThreadLocal} storing a {@link DoFn}, but
 * calls the {@link DoFn} {@link Setup @Setup} method the first time the {@link DoFn} is obtained
 * and {@link Teardown @Teardown} whenever the {@link DoFn} is removed, and provides a method for
 * clearing all cached {@link DoFn DoFns}.
 */
class DoFnLifecycleManager {
  public static DoFnLifecycleManager of(DoFn<?, ?> original) {
    return new DoFnLifecycleManager(original, null, null);
  }

  public static DoFnLifecycleManager of(DoFn<?, ?> original,
                                        AppliedPTransform<?, ?, ?> app,
                                        EvaluationContext ctx) {
    return new DoFnLifecycleManager(original, ctx, app);
  }

  private final LoadingCache<Thread, DoFn<?, ?>> outstanding;
  private final ConcurrentMap<Thread, Exception> thrownOnTeardown;

  private DoFnLifecycleManager(DoFn<?, ?> original, EvaluationContext ctx,
                               AppliedPTransform<?, ?, ?> app) {
    this.outstanding = CacheBuilder.newBuilder()
        .removalListener(new TeardownRemovedFnListener(ctx, app))
        .build(new DeserializingCacheLoader(original, app, ctx));
    thrownOnTeardown = new ConcurrentHashMap<>();
  }

  public <InputT, OutputT> DoFn<InputT, OutputT> get() throws Exception {
    Thread currentThread = Thread.currentThread();
    return (DoFn<InputT, OutputT>) outstanding.get(currentThread);
  }

  public void remove() throws Exception {
    Thread currentThread = Thread.currentThread();
    outstanding.invalidate(currentThread);
    // Block until the invalidate is fully completed
    outstanding.cleanUp();
    // Remove to try too avoid reporting the same teardown exception twice. May still double-report,
    // but the second will be suppressed.
    Exception thrown = thrownOnTeardown.remove(currentThread);
    if (thrown != null) {
      throw thrown;
    }
  }

  /**
   * Remove all {@link DoFn DoFns} from this {@link DoFnLifecycleManager}. Returns all exceptions
   * that were thrown while calling the remove methods.
   *
   * <p>If the returned Collection is nonempty, an exception was thrown from at least one {@link
   * DoFn.Teardown @Teardown} method, and the {@link PipelineRunner} should throw an exception.
   */
  public Collection<Exception> removeAll() throws Exception {
    outstanding.invalidateAll();
    // Make sure all of the teardowns are run
    outstanding.cleanUp();
    return thrownOnTeardown.values();
  }

  private static class DeserializingCacheLoader extends CacheLoader<Thread, DoFn<?, ?>> {
    private final byte[] original;
    private final EvaluationContext context;
    private final AppliedPTransform<?, ?, ?> application;

    public DeserializingCacheLoader(DoFn<?, ?> original,
                                    AppliedPTransform<?, ?, ?> app,
                                    EvaluationContext ctx) {
      this.original = SerializableUtils.serializeToByteArray(original);
      this.context = ctx;
      this.application = app;
    }

    @Override
    public DoFn<?, ?> load(Thread key) throws Exception {
      final DoFn<?, ?> fn = (DoFn<?, ?>) SerializableUtils.deserializeFromByteArray(original,
          "DoFn Copy in thread " + key.getName());
      final DoFnInvoker<?, ?> fnInvoker = DoFnInvokers.invokerFor(fn);
      fnInvoker.invokeSetup();
      if (context != null) {
        final DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());
        final DoFnSignature.LifecycleMethod teardown = signature.teardown();
        if (teardown != null) {
          context.registerFn(application, key.getId());
        }
      }
      return fn;
    }
  }

  private class TeardownRemovedFnListener implements RemovalListener<Thread, DoFn<?, ?>> {
    private final EvaluationContext context;
    private final AppliedPTransform<?, ?, ?> application;

    private TeardownRemovedFnListener(final EvaluationContext context,
                                      final AppliedPTransform<?, ?, ?> application) {
      this.context = context;
      this.application = application;
    }

    @Override
    public void onRemoval(RemovalNotification<Thread, DoFn<?, ?>> notification) {
      try {
        DoFnInvokers.invokerFor(notification.getValue()).invokeTeardown();
      } catch (Exception e) {
        thrownOnTeardown.put(notification.getKey(), e);
      } finally {
        if (context != null) {
          context.unregisterFn(application, notification.getKey().getId());
        }
      }
    }
  }
}
