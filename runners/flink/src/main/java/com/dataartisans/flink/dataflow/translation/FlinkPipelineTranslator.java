/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow.translation;

import com.google.cloud.dataflow.sdk.Pipeline;

/**
 * The role of this class is to translate the Beam operators to
 * their Flink counterparts. If we have a streaming job, this is instantiated as a
 * {@link FlinkStreamingPipelineTranslator}. In other case, i.e. for a batch job,
 * a {@link FlinkBatchPipelineTranslator} is created. Correspondingly, the
 * {@link com.google.cloud.dataflow.sdk.values.PCollection}-based user-provided job is translated into
 * a {@link org.apache.flink.streaming.api.datastream.DataStream} (for streaming) or a
 * {@link org.apache.flink.api.java.DataSet} (for batch) one.
 */
public abstract class FlinkPipelineTranslator implements Pipeline.PipelineVisitor {

  public void translate(Pipeline pipeline) {
    pipeline.traverseTopologically(this);
  }
}
