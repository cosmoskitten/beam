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

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.ReadPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.sdk.util.ReleaseInfo;

/**
 * Utilities for interacting with portability {@link Environment environments}.
 */
public class Environments {
  private static final Map<String, SdkFunctionSpecExtractor> KNOWN_URN_SPEC_EXTRACTORS =
      ImmutableMap.<String, SdkFunctionSpecExtractor>builder()
          .put(
              PTransformTranslation.COMBINE_TRANSFORM_URN,
              (transformSpec) -> CombinePayload.parseFrom(transformSpec).getCombineFn())
          .put(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              (transformSpec) -> ParDoPayload.parseFrom(transformSpec).getDoFn())
          .put(
              PTransformTranslation.READ_TRANSFORM_URN,
              (transformSpec) -> ReadPayload.parseFrom(transformSpec).getSource())
          .put(
              PTransformTranslation.WINDOW_TRANSFORM_URN,
              (transformPayload -> WindowIntoPayload.parseFrom(transformPayload).getWindowFn()))
          .build();
  private static final SdkFunctionSpecExtractor DEFAULT_SPEC_EXTRACTOR = transformPayload -> null;

  private static final String JAVA_SDK_HARNESS_CONTAINER_URL =
      String.format(
          "%s-%s",
          ReleaseInfo.getReleaseInfo().getName(), ReleaseInfo.getReleaseInfo().getVersion());
  public static final Environment JAVA_SDK_HARNESS_ENVIRONMENT =
      Environment.newBuilder().setUrl(JAVA_SDK_HARNESS_CONTAINER_URL).build();

  private Environments() {}

  @Nullable
  public static Environment getEnvironment(
      PTransform ptransform, RehydratedComponents components) throws IOException {
    SdkFunctionSpecExtractor specExtractor =
        KNOWN_URN_SPEC_EXTRACTORS.getOrDefault(
            ptransform.getSpec().getUrn(), DEFAULT_SPEC_EXTRACTOR);
    SdkFunctionSpec fnSpec = specExtractor.getSdkFunctionSpec(ptransform.getSpec().getPayload());
    if (fnSpec != null) {
      return components.getEnvironment(fnSpec.getEnvironmentId());
    } else {
      return null;
    }
  }

  /**
   * Extracts the {@link SdkFunctionSpec} for a {@link PTransform} payload, or {@code null} if there
   * is no user fn within the payload.
   */
  private interface SdkFunctionSpecExtractor {
    @Nullable
    SdkFunctionSpec getSdkFunctionSpec(ByteString transformPayload) throws IOException;
  }
}
