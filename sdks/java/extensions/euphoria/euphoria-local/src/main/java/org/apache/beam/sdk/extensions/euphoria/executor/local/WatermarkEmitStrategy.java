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
package org.apache.beam.sdk.extensions.euphoria.executor.local;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Strategy for emitting watermarks. */
public interface WatermarkEmitStrategy {

  /**
   * Schedule for periodic emitting.
   *
   * @param action function to be invoked periodically; must not be {@code null}
   */
  void schedule(Runnable action);

  /** Default strategy used in local executor. */
  class Default implements WatermarkEmitStrategy {

    private static final ScheduledExecutorService scheduler =
        new ScheduledThreadPoolExecutor(
            1, new ThreadFactoryBuilder().setNameFormat("watermark-%d").setDaemon(true).build());

    @Override
    public void schedule(Runnable action) {
      scheduler.scheduleAtFixedRate(action, 100, 100, TimeUnit.MILLISECONDS);
    }
  }
}
