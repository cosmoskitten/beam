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

import java.util.Collection;
import java.util.Optional;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;

/**
 * A combination of PTransforms that can be executed within a single SDK harness.
 *
 * <p>Contains only the nodes that specify the processing to perform within the harness, and does
 * not contain.
 *
 * <p>TODO: All PCollection reads are (Remote_read XOR Locally_produced). PCollection writes are
 * (Local_write OR Remote_write), unless I figure out the right way to insert an identity transform
 * URN. TODO: Implement an Identity Transform in every SDK harness and the associated URN
 */
public interface ExecutableSubgraph {
  String URN = "urn:beam:runner:stage:v1";
  /**
   * Returns the {@link Environment} this stage executes in.
   *
   * <p>An {@link ExecutableSubgraph} consists of {@link PTransform PTransforms} which can all be
   * executed within a single {@link Environment}. The assumption made here is that
   * runner-implemented transforms will be associated with these subgraphs by the overall graph
   * topology, which
   */
  Environment getEnvironment();

  /**
   * Returns the root {@link PCollectionNode} of this {@link ExecutableSubgraph}. If the returned
   * value is present, this {@link ExecutableSubgraph} executes by reading elements from a Remote
   * GRPC Read Node. If the returned value is absent, the {@link ExecutableSubgraph} executes by
   * reading from a single Read node (which contains a {@link ReadTranslation read transform}).
   */
  Optional<PCollectionNode> getConsumedPCollection();

  /**
   * Returns the leaf {@link PCollectionNode PCollections} of this {@link ExecutableSubgraph}. At
   * execution time, all of these {@link PCollectionNode PCollections} must be materialized by a
   * Remote GRPC Write Transform.
   */
  Collection<PCollectionNode> getMaterializedPCollections();

  /**
   * Returns a composite {@link PTransform} which contains as {@link
   * PTransform#getSubtransformsList() subtransforms} all of the {@link PTransform PTransforms}
   * fused into this {@link ExecutableSubgraph}.
   *
   * <p>The input {@link PCollection} for the returned {@link PTransform} will be the consumed
   * {@link PCollectionNode} returned by {@link #getConsumedPCollection()} and the output {@link
   * PCollection PCollections} will be the {@link PCollectionNode PCollections} returned by {@link
   * #getMaterializedPCollections()}.
   */
  PTransform toPTransform();
}
