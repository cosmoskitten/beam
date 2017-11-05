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
package org.apache.beam.sdk.io.aws.options;

import com.fasterxml.jackson.annotation.JsonIgnore;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.aws.util.S3Util;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;

/**
 * Options used to configure Amazon Web Services S3.
 */
public interface S3Options extends AwsOptions {

  @JsonIgnore
  @Description("The S3Util instance that should be used to communicate with AWS S3")
  @Default.InstanceFactory(S3Util.S3UtilFactory.class)
  @Hidden
  S3Util getS3Util();

  void setS3Util(S3Util value);

  @Description("AWS S3 storage class used for creating S3 objects")
  @Default.String("STANDARD")
  String getS3StorageClass();

  void setS3StorageClass(String value);

  @Description("Size of S3 upload chunks; max upload object size is this value multiplied by 10000")
  @Nullable
  Integer getS3UploadBufferSizeBytes();

  void setS3UploadBufferSizeBytes(Integer value);

  @Description("Thread pool size, limiting max concurrent S3 operations")
  @Default.Integer(50)
  int getS3ThreadPoolSize();

  void setS3ThreadPoolSize(int value);
}
