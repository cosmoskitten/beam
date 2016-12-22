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

package org.apache.beam.runners.spark.coders;

import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;


/**
 * {@link org.apache.beam.sdk.coders.Coder} wrapper for serialization purposes.
 * See: {@link BeamSparkRunnerRegistrator}.
 */
public class CoderWrapper<T> implements Serializable {
  private final Coder<T> coder;

  public CoderWrapper(Coder<T> coder) {
    this.coder = coder;
  }

  Coder<T> getCoder() {
    return coder;
  }
}
