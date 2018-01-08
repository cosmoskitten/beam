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
import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.DoFn.Teardown;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
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

  private final LoadingCache<Thread, Value> outstanding;
  private final ConcurrentMap<Thread, Exception> thrownOnTeardown;

  private DoFnLifecycleManager(DoFn<?, ?> original, EvaluationContext ctx,
                               AppliedPTransform<?, ?, ?> app) {
    this.outstanding = CacheBuilder.newBuilder()
        .removalListener(new TeardownRemovedFnListener(ctx))
        .build(new DeserializingCacheLoader(original, app));
    thrownOnTeardown = new ConcurrentHashMap<>();
    if (ctx != null && app != null) {
      ctx.registerFn(toKey(app));
    }
  }

  // can we make AppliedPTransform Serializable and use it as key?
  private static String toKey(AppliedPTransform<?, ?, ?> app) {
    return app.getFullName();
  }

  public <InputT, OutputT> DoFn<InputT, OutputT> get() throws Exception {
    Thread currentThread = Thread.currentThread();
    return (DoFn<InputT, OutputT>) outstanding.get(currentThread).original;
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

  private static class Value implements Serializable {
    private final DoFn<?, ?> original;
    private final String key;

    private Value(final DoFn<?, ?> original, final AppliedPTransform<?, ?, ?> app) {
      this.original = original;
      this.key = app == null ? "" : toKey(app);
    }
  }

  private static class DeserializingCacheLoader extends CacheLoader<Thread, Value> {
    private final byte[] original;

    public DeserializingCacheLoader(DoFn<?, ?> original, AppliedPTransform<?, ?, ?> app) {
      this.original = SerializableUtils.serializeToByteArray(new Value(original, app));
    }

    @Override
    public Value load(Thread key) throws Exception {
      Value fn = (Value) SerializableUtils.deserializeFromByteArray(original,
          "DoFn Copy in thread " + key.getName());
      DoFnInvokers.invokerFor(fn.original).invokeSetup();
      return fn;
    }
  }

  private class TeardownRemovedFnListener implements RemovalListener<Thread, Value> {
    private final EvaluationContext context;

    private TeardownRemovedFnListener(final EvaluationContext context) {
      this.context = context;
    }

    @Override
    public void onRemoval(RemovalNotification<Thread, Value> notification) {
      try {
        DoFnInvokers.invokerFor(notification.getValue().original).invokeTeardown();
      } catch (Exception e) {
        thrownOnTeardown.put(notification.getKey(), e);
      } finally {
        if (context != null) {
          context.unregisterFn(notification.getValue().key);
        }
      }
    }
  }
}
