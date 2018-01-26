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

package org.apache.beam.runners.core.construction;

import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;

/**
 * An interface that translates coders to components and back.
 *
 * <p>This interface is highly experimental, and incomplete. Coders must in the general case have
 * the capability to encode an additional payload, which is not currently supported. This exists as
 * a temporary measure.
 */
@Experimental(Kind.CORE_RUNNERS_ONLY)
public interface CoderTranslator<T extends Coder<?>> {
  /**
  * Extract all component {@link Coder coders} within a coder.
   */
  List<? extends Coder<?>> getComponents(T from);

  /**
   * Returns the
   */
  byte[] getPayload(T from);

  /** Create a {@link Coder} from its component {@link Coder coders}. */
  T fromComponents(List<Coder<?>> components, byte[] payload);
}
