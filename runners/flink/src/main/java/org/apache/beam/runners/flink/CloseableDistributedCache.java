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
package org.apache.beam.runners.flink;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.flink.api.common.cache.DistributedCache;

/**
 * A distributed cache that can be closed. Downstream lifetime observers will be notified when
 * closed. Note thread-safe.
 */
public class CloseableDistributedCache implements ManagedDistributedCache, AutoCloseable {
  private final DistributedCache distributedCache;
  private final List<ThrowingRunnable> closeListeners = Lists.newArrayList();

  private boolean isClosed = false;

  public static CloseableDistributedCache wrapping(DistributedCache distributedCache) {
    checkArgument(distributedCache != null);
    return new CloseableDistributedCache(distributedCache);
  }

  private CloseableDistributedCache(DistributedCache distributedCache) {
    this.distributedCache = distributedCache;
  }

  @Override
  public DistributedCache getDistributedCache() {
    return distributedCache;
  }

  @Override
  public void addCloseListener(ThrowingRunnable onClose) {
    closeListeners.add(onClose);
  }

  @Override
  public void close() throws Exception {
    if (!isClosed) {
      isClosed = true;
      for (ThrowingRunnable listener : closeListeners) {
        listener.run();
      }
    }
  }
}
