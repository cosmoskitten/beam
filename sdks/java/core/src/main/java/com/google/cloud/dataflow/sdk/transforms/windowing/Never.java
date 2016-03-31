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
package com.google.cloud.dataflow.sdk.transforms.windowing;

import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;

import org.joda.time.Instant;

import java.util.List;

/**
 * A trigger which never fires. Any {@link GroupByKey} that {@link Never} triggers will produce
 * output for a {@link BoundedWindow} only when that {@link BoundedWindow window} closes.
 */
public class Never {
  /**
   * Returns a trigger which never fires. Output will be produced from the using {@link GroupByKey}
   * when the {@link BoundedWindow} closes.
   */
  public static <W extends BoundedWindow> OnceTrigger<W> ever() {
    // NeverTrigger ignores all inputs and is Window-type independent.
    return new NeverTrigger<>();
  }

  private static class NeverTrigger<W extends BoundedWindow> extends OnceTrigger<W> {
    protected NeverTrigger() {
      super(null);
    }

    @Override
    public void onElement(OnElementContext c) throws Exception {}

    @Override
    public void onMerge(OnMergeContext c) throws Exception {}

    @Override
    protected Trigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
      return this;
    }

    @Override
    public Instant getWatermarkThatGuaranteesFiring(W window) {
      return BoundedWindow.TIMESTAMP_MAX_VALUE;
    }

    @Override
    public boolean shouldFire(Trigger<W>.TriggerContext context) throws Exception {
      return false;
    }

    @Override
    protected void onOnlyFiring(Trigger<W>.TriggerContext context) throws Exception {
      throw new UnsupportedOperationException(
          String.format("%s should never fire", getClass().getSimpleName()));
    }
  }
}
