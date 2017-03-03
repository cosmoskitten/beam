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

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * This test checks how the cache candidates map is populated by the runner when evaluating the
 * pipeline.
 */
public class CacheTest {

  @Test
  public void dagPreVisitCacheTest() throws Exception {
    SparkPipelineOptions options = PipelineOptionsFactory.create().as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    Pipeline pipeline = Pipeline.create(options);
    PCollection pcollection = pipeline.apply(Create.of("foo", "bar"));
    // first read
    pcollection.apply(Count.globally());
    // second read
    pcollection.apply(Count.globally());

    SparkRunner.DAGPreVisit dagPreVisit = new SparkRunner.DAGPreVisit();
    pipeline.traverseTopologically(dagPreVisit);
    assertEquals(2L, (long) dagPreVisit.getCacheCandidates().get(pcollection));
  }

}
