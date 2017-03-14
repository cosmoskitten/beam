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

package org.apache.beam.runners.gearpump.translators;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.values.PValue;

import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStreamApp;
import org.apache.gearpump.streaming.source.DataSource;

/**
 * Maintains context data for {@link TransformTranslator}s.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TranslationContext {

  private final JavaStreamApp streamApp;
  private final GearpumpPipelineOptions pipelineOptions;
  private AppliedPTransform<?, ?, ?> currentTransform;
  private final Map<PValue, JavaStream<?>> streams = new HashMap<>();

  public TranslationContext(JavaStreamApp streamApp, GearpumpPipelineOptions pipelineOptions) {
    this.streamApp = streamApp;
    this.pipelineOptions = pipelineOptions;
  }

  public void setCurrentTransform(TransformHierarchy.Node treeNode) {
    this.currentTransform = treeNode.toAppliedPTransform();
  }

  public GearpumpPipelineOptions getPipelineOptions() {
    return pipelineOptions;
  }

  public <InputT> JavaStream<InputT> getInputStream(PValue input) {
    return (JavaStream<InputT>) streams.get(input);
  }

  public <OutputT> void setOutputStream(PValue output, JavaStream<OutputT> outputStream) {
    if (!streams.containsKey(output)) {
      streams.put(output, outputStream);
    }
  }

  public List<TaggedPValue> getInputs() {
    return getCurrentTransform().getInputs();
  }

  public PValue getInput() {
    return Iterables.getOnlyElement(getInputs()).getValue();
  }

  public List<TaggedPValue> getOutputs() {
    return getCurrentTransform().getOutputs();
  }

  public PValue getOutput() {
    return Iterables.getOnlyElement(getOutputs()).getValue();
  }

  private AppliedPTransform<?, ?, ?> getCurrentTransform() {
    checkArgument(
        currentTransform != null,
        "current transform not set");
    return currentTransform;
  }

  public <T> JavaStream<T> getSourceStream(DataSource dataSource) {
    return streamApp.source(dataSource, pipelineOptions.getParallelism(),
        UserConfig.empty(), "source");
  }

}
