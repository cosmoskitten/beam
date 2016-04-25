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
package ${package}.common;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Options that can be used to configure Pub/Sub topic in Dataflow examples.
 */
public interface ExamplePubsubTopicOptions extends DataflowPipelineOptions {
  @Description("Pub/Sub topic")
  @Default.InstanceFactory(PubsubTopicFactory.class)
  String getPubsubTopic();
  void setPubsubTopic(String topic);

  @Description("Number of workers to use when executing the injector pipeline")
  @Default.Integer(1)
  int getInjectorNumWorkers();
  void setInjectorNumWorkers(int numWorkers);

  /**
   * Returns a default Pub/Sub topic based on the project and the job names.
   */
  static class PubsubTopicFactory implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      DataflowPipelineOptions dataflowPipelineOptions =
          options.as(DataflowPipelineOptions.class);
      return "projects/" + dataflowPipelineOptions.getProject()
          + "/topics/" + dataflowPipelineOptions.getJobName();
    }
  }
}
