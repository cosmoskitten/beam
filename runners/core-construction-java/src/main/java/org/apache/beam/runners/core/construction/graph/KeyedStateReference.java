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

package org.apache.beam.runners.core.construction.graph;

import com.google.auto.value.AutoValue;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.KeyedStateId;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;

/**
 * A reference to keyed state. This includes the PTransform that references the user state as well
 * as the local name. Both are necessary in order to fully resolve user state.
 */
@AutoValue
public abstract class KeyedStateReference {

  /** Create a keyed state reference. */
  public static KeyedStateReference of(
      PTransformNode transform, String localName, PCollectionNode collection) {
    return new AutoValue_KeyedStateReference(transform, localName, collection);
  }

  /** Create a keyed state reference from a KeyedStateId proto and components. */
  public static KeyedStateReference fromKeyedStateId(
      KeyedStateId keyedStateId, RunnerApi.Components components) {
    String transformId = keyedStateId.getTransformId();
    String localName = keyedStateId.getLocalName();
    String collectionId = components.getTransformsOrThrow(transformId).getInputsOrThrow(localName);
    PTransform transform = components.getTransformsOrThrow(transformId);
    PCollection collection = components.getPcollectionsOrThrow(collectionId);
    return KeyedStateReference.of(
        PipelineNode.pTransform(transformId, transform),
        localName,
        PipelineNode.pCollection(collectionId, collection));
  }

  /** The id of the PTransform that uses this keyed state. */
  public abstract PTransformNode transform();
  /** The local name the referencing PTransform uses to refer to this keyed state. */
  public abstract String localName();
  /** The PCollection that represents the input to the PTransform. */
  public abstract PCollectionNode collection();
}
