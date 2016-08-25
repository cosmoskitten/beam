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
package org.apache.beam.sdk.util.state;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * A factory for providing {@link StateInternals} for a particular key.
 *
 * <p>Because it will generally be embedded in a {@link org.apache.beam.sdk.transforms.DoFn DoFn},
 * albeit at execution time, it is marked {@link Serializable}.
 */
@Experimental(Kind.STATE)
public interface StateInternalsFactory<K> {

  /** Returns {@link StateInternals} for the provided key. */
  StateInternals<K> stateInternalsForKey(K key);
}
