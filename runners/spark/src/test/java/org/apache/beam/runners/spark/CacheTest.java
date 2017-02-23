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
package org.apache.beam.runners.spark;

import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PCollection;

/**
 * This test checks how the cache candidates is populated by the Spark runner when evaluating the
 * pipeline.
 */
public class CacheTest {

  /**
   * An evaluator used to get the cache candidates populated during DAGPreVisit and test if the
   * cache is correctly enabled.
   */
  class TestEvaluator extends Pipeline.PipelineVisitor.Defaults {

    private final Map<PCollection, Long> cacheCandidates;

    public TestEvaluator(Map<PCollection, Long> cacheCandidates) {
      this.cacheCandidates = cacheCandidates;
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      // TODO
    }

  }

}
