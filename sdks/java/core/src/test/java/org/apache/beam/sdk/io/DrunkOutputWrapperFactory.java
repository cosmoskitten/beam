/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.io.FileBasedSink.OutputWrapper;
import org.apache.beam.sdk.io.FileBasedSink.OutputWrapperFactory;
import org.apache.beam.sdk.util.MimeTypes;

/**
 * {@link OutputWrapperFactory} implementation useful for testing that creates an
 * {@link OutputWrapper} that writes everything twice and finishes with an extra two lines, both
 * containing the text {@literal Finito!}.
 */
public class DrunkOutputWrapperFactory implements OutputWrapperFactory {
  @Override
  public OutputWrapper create(WritableByteChannel channel) throws IOException {
    return new SimpleOutputWrapper(channel);
  }

  @Override
  public String getMimeType() {
    return MimeTypes.TEXT;
  }

  @Override
  public String getFilenameSuffix() {
    return ".drunk";
  }

  /**
   * OutputWrapper that writes everything twice.
   */
  private static class SimpleOutputWrapper extends OutputWrapper {
    public SimpleOutputWrapper(final WritableByteChannel channel) {
      super(channel);
    }

    @Override
    public void finish() throws IOException {
      write(ByteBuffer.wrap("Finito!\n".getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      final int w1 = super.write(src);
      src.rewind();
      final int w2 = super.write(src);
      return w1 + w2;
    }
  }
}
