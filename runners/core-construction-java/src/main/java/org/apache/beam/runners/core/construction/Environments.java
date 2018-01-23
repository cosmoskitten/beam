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
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.ReadPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.sdk.util.ReleaseInfo;

/**
 * Utilities for interacting with portability {@link Environment environments}.
 */
public class Environments {
  private static final Map<String, EnvironmentIdExtractor> KNOWN_URN_SPEC_EXTRACTORS =
      ImmutableMap.<String, EnvironmentIdExtractor>builder()
          .put(PTransformTranslation.COMBINE_TRANSFORM_URN, Environments::combineExtractor)
          .put(PTransformTranslation.PAR_DO_TRANSFORM_URN, Environments::parDoExtractor)
          .put(PTransformTranslation.READ_TRANSFORM_URN, Environments::readExtractor)
          .put(PTransformTranslation.WINDOW_TRANSFORM_URN, Environments::windowExtractor)
          .put(PTransformTranslation.FLATTEN_TRANSFORM_URN, Environments::flattenExtractor)
          .build();

  private static final EnvironmentIdExtractor DEFAULT_SPEC_EXTRACTOR = (transform) -> null;

  private static final String JAVA_SDK_HARNESS_CONTAINER_URL =
      String.format(
          "%s-%s",
          ReleaseInfo.getReleaseInfo().getName(), ReleaseInfo.getReleaseInfo().getVersion());
  public static final Environment JAVA_SDK_HARNESS_ENVIRONMENT =
      Environment.newBuilder().setUrl(JAVA_SDK_HARNESS_CONTAINER_URL).build();

  private Environments() {}

  @Nullable
  public static Environment getEnvironment(
      PTransform ptransform, RehydratedComponents components) {
    try {
      String envId =
          KNOWN_URN_SPEC_EXTRACTORS
              .getOrDefault(ptransform.getSpec().getUrn(), DEFAULT_SPEC_EXTRACTOR)
              .getEnvironmentId(ptransform);
      if (envId != null) {
        return components.getEnvironment(envId);
      } else {
        return null;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private interface EnvironmentIdExtractor {
    String getEnvironmentId(PTransform transform)
        throws IOException;
  }

  private static String parDoExtractor(PTransform pTransform)
      throws InvalidProtocolBufferException {
    return ParDoPayload.parseFrom(pTransform.getSpec().getPayload()).getDoFn().getEnvironmentId();
  }

  private static String combineExtractor(PTransform pTransform)
      throws InvalidProtocolBufferException {
    return CombinePayload.parseFrom(pTransform.getSpec().getPayload())
        .getCombineFn()
        .getEnvironmentId();
  }

  private static String readExtractor(PTransform transform)
      throws InvalidProtocolBufferException {
    return ReadPayload.parseFrom(transform.getSpec().getPayload()).getSource().getEnvironmentId();
  }

  private static String windowExtractor(PTransform transform)
      throws InvalidProtocolBufferException {
    return WindowIntoPayload.parseFrom(transform.getSpec().getPayload())
        .getWindowFn()
        .getEnvironmentId();
  }

  private static String flattenExtractor(PTransform transform) {
    // TODO: If all upstream PTransforms are in the same environment, the Flatten can be executed
    // in the same environment as those transforms. This requires inspecting the topology of the
    // graph, however, and that information is not currently available in this context. This may
    // performing this extraction recursively (e.g. if an input transform is also a flatten)
    return null;
  }
}
