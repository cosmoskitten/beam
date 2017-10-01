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
package org.apache.beam.sdk.io.aws.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.aws.s3.S3Path;

/** A writable S3 object, as a {@link WritableByteChannel}. */
class S3WritableByteChannel implements WritableByteChannel {

  private final AmazonS3 amazonS3;
  private final S3Path path;
  private final String uploadId;
  private final ByteArrayOutputStream uploadStream;
  private final WritableByteChannel uploadChannel;
  private final List<PartETag> eTags;

  // Amazon parts are 1-indexed, not zero-indexed.
  private int partNumber = 1;
  private boolean open = true;

  S3WritableByteChannel(
      AmazonS3 amazonS3, S3Path path, String storageClass, int uploadBufferSizeBytes)
      throws IOException {
    this.amazonS3 = checkNotNull(amazonS3, "amazonS3");
    this.path = checkNotNull(path, "path");
    checkArgument(uploadBufferSizeBytes > 0, "uploadBufferSizeBytes");
    this.uploadStream = new ByteArrayOutputStream(uploadBufferSizeBytes);
    this.uploadChannel = Channels.newChannel(uploadStream);
    eTags = new ArrayList<>();

    InitiateMultipartUploadRequest request =
        new InitiateMultipartUploadRequest(path.getBucket(), path.getKey())
            .withStorageClass(storageClass);
    InitiateMultipartUploadResult result;
    try {
      result = amazonS3.initiateMultipartUpload(request);
    } catch (AmazonServiceException e) {
      throw new IOException(e);
    }
    uploadId = result.getUploadId();
  }

  @Override
  public int write(ByteBuffer sourceBuffer) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    if (!sourceBuffer.hasArray()) {
      throw new IllegalArgumentException("sourceBuffer is expected to have a backing array");
    }

    int totalBytesWritten = 0;
    while (sourceBuffer.hasRemaining()) {
      int bytesWritten = uploadChannel.write(sourceBuffer);
      totalBytesWritten += bytesWritten;
      if (sourceBuffer.hasRemaining()) {
        flush();
      }
    }

    return totalBytesWritten;
  }

  private void flush() throws IOException {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(uploadStream.toByteArray());

    UploadPartRequest request =
        new UploadPartRequest()
            .withBucketName(path.getBucket())
            .withKey(path.getKey())
            .withUploadId(uploadId)
            .withPartNumber(partNumber++)
            .withPartSize(uploadStream.size())
            .withInputStream(inputStream);
    UploadPartResult result;
    try {
      result = amazonS3.uploadPart(request);
    } catch (AmazonServiceException e) {
      throw new IOException(e);
    }
    uploadStream.reset();
    eTags.add(result.getPartETag());
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() throws IOException {
    open = false;
    if (uploadStream.size() > 0) {
      flush();
    }
    CompleteMultipartUploadRequest request =
        new CompleteMultipartUploadRequest()
            .withBucketName(path.getBucket())
            .withKey(path.getKey())
            .withUploadId(uploadId)
            .withPartETags(eTags);
    try {
      amazonS3.completeMultipartUpload(request);
    } catch (AmazonServiceException e) {
      throw new IOException(e);
    }
  }
}
