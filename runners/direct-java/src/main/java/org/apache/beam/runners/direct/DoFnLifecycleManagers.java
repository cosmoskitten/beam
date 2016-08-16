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

import java.util.ArrayList;
import java.util.Collection;

/**
 * Utility methods for interacting with {@link DoFnLifecycleManager DoFnLifecycleManagers}.
 */
class DoFnLifecycleManagers {
  private DoFnLifecycleManagers() {
    /* Do not instantiate */
  }

  static void removeAllFromManagers(Iterable<DoFnLifecycleManager> managers) throws Exception {
    Collection<Exception> thrown = new ArrayList<>();
    for (DoFnLifecycleManager manager : managers) {
      thrown.addAll(manager.removeAll());
    }
    if (!thrown.isEmpty()) {
      Exception overallException = new Exception("Exceptions thrown while tearing down DoFns");
      for (Exception e : thrown) {
        overallException.addSuppressed(e);
      }
      throw overallException;
    }
  }
}
