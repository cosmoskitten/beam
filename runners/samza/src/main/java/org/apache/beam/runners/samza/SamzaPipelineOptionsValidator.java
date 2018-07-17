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

package org.apache.beam.runners.samza;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;

/** Validates that the {@link SamzaPipelineOptions} conforms to all the criteria. */
public class SamzaPipelineOptionsValidator {
  public static SamzaPipelineOptions validate(PipelineOptions opts) {
    final SamzaPipelineOptions samzaOptions =
        PipelineOptionsValidator.validate(SamzaPipelineOptions.class, opts);

    checkArgument(samzaOptions.getMaxSourceParallelism() >= 1);
    return samzaOptions;
  }
}
