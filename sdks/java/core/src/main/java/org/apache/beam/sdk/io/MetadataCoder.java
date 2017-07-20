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
package org.apache.beam.sdk.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.fs.MatchResult;

/** A {@link Coder} for {@link org.apache.beam.sdk.io.fs.MatchResult.Metadata}. */
public class MetadataCoder extends AtomicCoder<MatchResult.Metadata> {
  private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();
  private static final VarIntCoder INT_CODER = VarIntCoder.of();
  private static final VarLongCoder LONG_CODER = VarLongCoder.of();

  /** Creates a {@link MetadataCoder}. */
  public static MetadataCoder of() {
    return new MetadataCoder();
  }

  @Override
  public void encode(MatchResult.Metadata value, OutputStream os) throws IOException {
    STRING_CODER.encode(value.resourceId().toString(), os);
    INT_CODER.encode(value.isReadSeekEfficient() ? 1 : 0, os);
    LONG_CODER.encode(value.sizeBytes(), os);
  }

  @Override
  public MatchResult.Metadata decode(InputStream is) throws IOException {
    String resourceId = STRING_CODER.decode(is);
    boolean isReadSeekEfficient = INT_CODER.decode(is) == 1;
    long sizeBytes = LONG_CODER.decode(is);
    return MatchResult.Metadata.builder()
        .setResourceId(FileSystems.matchNewResource(resourceId, false /* isDirectory */))
        .setIsReadSeekEfficient(isReadSeekEfficient)
        .setSizeBytes(sizeBytes)
        .build();
  }
}
