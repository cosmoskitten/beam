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
package org.apache.beam.runners.dataflow.options;

import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.IOException;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.util.DefaultBucket;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.CloudResourceManagerOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PubsubOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Options that can be used to configure the {@link DataflowRunner}.
 */
@Description("Options that configure the Dataflow pipeline.")
public interface DataflowPipelineOptions
    extends PipelineOptions, GcpOptions, ApplicationNameOptions, DataflowPipelineDebugOptions,
        DataflowPipelineWorkerPoolOptions, BigQueryOptions, GcsOptions, StreamingOptions,
        CloudDebuggerOptions, DataflowWorkerLoggingOptions, DataflowProfilingOptions,
        PubsubOptions, CloudResourceManagerOptions {

  static final Logger LOG = LoggerFactory.getLogger(DataflowPipelineOptions.class);

  @Description("Project id. Required when running a Dataflow in the cloud. "
      + "See https://cloud.google.com/storage/docs/projects for further details.")
  @Override
  @Validation.Required
  @Default.InstanceFactory(DefaultProjectFactory.class)
  String getProject();
  @Override
  void setProject(String value);

  /**
   * GCS path for staging local files, e.g. gs://bucket/object
   *
   * <p>Must be a valid Cloud Storage URL, beginning with the prefix "gs://"
   *
   * <p>If {@link #getStagingLocation()} is not set, it will default to
   * {@link GcpOptions#getGcpTempLocation()}. {@link GcpOptions#getGcpTempLocation()}
   * must be a valid GCS path.
   */
  @Description("GCS path for staging local files, e.g. \"gs://bucket/object\". "
      + "Must be a valid Cloud Storage URL, beginning with the prefix \"gs://\". "
      + "If stagingLocation is unset, defaults to gcpTempLocation with \"/staging\" suffix.")
  @Default.InstanceFactory(StagingLocationFactory.class)
  String getStagingLocation();
  void setStagingLocation(String value);

  /**
   * Whether to update the currently running pipeline with the same name as this one.
   */
  @Description(
      "If set, replace the existing pipeline with the name specified by --jobName with "
          + "this pipeline, preserving state.")
  boolean isUpdate();
  void setUpdate(boolean value);

  /**
   * Returns a default staging location under {@link GcpOptions#getGcpTempLocation}.
   */
  public static class StagingLocationFactory implements DefaultValueFactory<String> {

    @Override
    public String create(PipelineOptions options) {
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      String gcpTempLocation = gcsOptions.getGcpTempLocation();

      if (isNullOrEmpty(gcpTempLocation)) {
        gcpTempLocation = DefaultBucket.tryCreateDefaultBucket(options);
        gcsOptions.setGcpTempLocation(gcpTempLocation);
      }
      try {
        gcsOptions.getPathValidator().validateOutputFilePrefixSupported(gcpTempLocation);
      } catch (Exception e) {
        throw new IllegalArgumentException(String.format(
            "Error constructing default value for stagingLocation: gcpTempLocation is not"
            + " a valid GCS path, %s. ", gcpTempLocation));
      }
      try {
        return IOChannelUtils.resolve(gcpTempLocation, "staging");
      } catch (IOException e) {
        throw new IllegalArgumentException(String.format(
            "Unable to resolve stagingLocation from gcpTempLocation: %s."
            + " Please set the staging location explicitly.", gcpTempLocation), e);
      }
    }
  }
}
