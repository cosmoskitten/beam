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
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import org.apache.beam.sdk.io.aws.s3.S3Path;

/**
 * A readable S3 object, as a {@link SeekableByteChannel}.
 */
class S3ReadableSeekableByteChannel implements SeekableByteChannel {

  private final AmazonS3 amazonS3;
  private final S3Path path;
  private final long contentLength;
  private long position = 0;
  private boolean open = true;
  private S3Object s3Object;
  private ReadableByteChannel s3ObjectContentChannel;

  S3ReadableSeekableByteChannel(AmazonS3 amazonS3, S3Path path) throws IOException {
    this.amazonS3 = checkNotNull(amazonS3, "amazonS3");
    this.path = checkNotNull(path, "path");
    try {
      contentLength = amazonS3.getObjectMetadata(path.getBucket(), path.getKey())
          .getContentLength();
    } catch (AmazonServiceException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int read(ByteBuffer destinationBuffer) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    if (!destinationBuffer.hasArray()) {
      throw new UnsupportedOperationException("ByteBuffer.hasArray() must be true");
    }
    if (!destinationBuffer.hasRemaining()) {
      return 0;
    }
    if (position == contentLength) {
      return -1;
    }

    if (s3Object == null) {
      GetObjectRequest request = new GetObjectRequest(path.getBucket(), path.getKey());
      if (position > 0) {
        request.setRange(position, contentLength);
      }
      try {
        s3Object = amazonS3.getObject(request);
      } catch (AmazonServiceException e) {
        throw new IOException(e);
      }
      s3ObjectContentChannel = Channels.newChannel(
          new BufferedInputStream(s3Object.getObjectContent(), 1024 * 1024));
    }

    int totalBytesRead = 0;
    int bytesRead = 0;

    do {
      totalBytesRead += bytesRead;
      bytesRead = s3ObjectContentChannel.read(destinationBuffer);
    } while (bytesRead > 0);

    position += totalBytesRead;
    return totalBytesRead;
  }

  @Override
  public long position() throws ClosedChannelException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    return position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    checkArgument(newPosition >= 0, "newPosition too low");
    checkArgument(newPosition < contentLength, "new position too high");
    if (s3Object != null) {
      s3Object.close();
    }
    position = newPosition;
    return this;
  }

  @Override
  public long size() throws ClosedChannelException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    return contentLength;
  }

  @Override
  public void close() throws IOException {
    if (s3Object != null) {
      s3Object.close();
    }
    open = false;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public int write(ByteBuffer src) {
    throw new NonWritableChannelException();
  }

  @Override
  public SeekableByteChannel truncate(long size) {
    throw new NonWritableChannelException();
  }
}
