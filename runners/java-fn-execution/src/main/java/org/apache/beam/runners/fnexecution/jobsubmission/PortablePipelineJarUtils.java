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
package org.apache.beam.runners.fnexecution.jobsubmission;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.jar.Attributes.Name;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Message.Builder;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.util.JsonFormat;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains common code for writing and reading portable pipeline jars.
 *
 * <p>Jar layout:
 *
 * <ul>
 *   <li>META-INF/
 *       <ul>
 *         <li>MANIFEST.MF
 *       </ul>
 *   <li>BEAM-PIPELINE/
 *       <ul>
 *         <li>[1st pipeline (default)]
 *             <ul>
 *               <li>pipeline.json
 *               <li>pipeline-options.json
 *               <li>artifact-manifest.json
 *               <li>artifacts/
 *                   <ul>
 *                     <li>...artifact files...
 *                   </ul>
 *             </ul>
 *         <li>[nth pipeline]
 *             <ul>
 *               Same as above
 *         </ul>
 *   </ul>
 *   <li>...Java classes...
 * </ul>
 */
public abstract class PortablePipelineJarUtils {
  private static final String ARTIFACT_FOLDER = "artifacts";
  private static final String PIPELINE_FOLDER = "BEAM-PIPELINE";
  private static final String ARTIFACT_MANIFEST = "artifact-manifest.json";
  private static final String PIPELINE = "pipeline.json";
  private static final String PIPELINE_OPTIONS = "pipeline-options.json";
  static final Name DEFAULT_BEAM_JOB = new Name("Default-Beam-Job");

  private static final Logger LOG = LoggerFactory.getLogger(PortablePipelineJarCreator.class);

  private static InputStream getResourceFromClassPath(String resourcePath) throws IOException {
    InputStream inputStream =
        PortablePipelineJarUtils.class.getClassLoader().getResourceAsStream(resourcePath);
    if (inputStream == null) {
      throw new FileNotFoundException(
          String.format("Resource %s not found on classpath.", resourcePath));
    }
    return inputStream;
  }

  /** Populates {@code builder} using the JSON resource specified by {@code resourcePath}. */
  private static void parseJsonResource(String resourcePath, Builder builder) throws IOException {
    try (InputStream inputStream = getResourceFromClassPath(resourcePath)) {
      String contents = new String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8);
      JsonFormat.parser().merge(contents, builder);
    }
  }

  public static Pipeline getPipelineFromClasspath(String jobName) throws IOException {
    Pipeline.Builder builder = Pipeline.newBuilder();
    parseJsonResource(getPipelineUri(jobName), builder);
    return builder.build();
  }

  public static Struct getPipelineOptionsFromClasspath(String jobName) throws IOException {
    Struct.Builder builder = Struct.newBuilder();
    parseJsonResource(getPipelineOptionsUri(jobName), builder);
    return builder.build();
  }

  public static String getArtifactManifestUri(String jobName) {
    return PIPELINE_FOLDER + "/" + jobName + "/" + ARTIFACT_MANIFEST;
  }

  static String getPipelineUri(String jobName) {
    return PIPELINE_FOLDER + "/" + jobName + "/" + PIPELINE;
  }

  static String getPipelineOptionsUri(String jobName) {
    return PIPELINE_FOLDER + "/" + jobName + "/" + PIPELINE_OPTIONS;
  }

  static String getArtifactUri(String jobName, String artifactId) {
    return PIPELINE_FOLDER + "/" + jobName + "/" + ARTIFACT_FOLDER + "/" + artifactId;
  }

  public static String getDefaultJobName() throws IOException {
    try (InputStream inputStream = getResourceFromClassPath(JarFile.MANIFEST_NAME)) {
      Manifest manifest = new Manifest(inputStream);
      return manifest.getMainAttributes().getValue(DEFAULT_BEAM_JOB);
    }
  }
}
