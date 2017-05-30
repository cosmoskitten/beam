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

package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.ParDoPayload;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite;

/** Tests for {@link ParDoTranslation}. */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  WriteFilesTranslationTest.TestWriteFilesPayloadTranslation.class,
})
public class WriteFilesTranslationTest {

  /** Tests for translating various {@link ParDo} transforms to/from {@link ParDoPayload} protos. */
  @RunWith(Parameterized.class)
  public static class TestWriteFilesPayloadTranslation {
    @Parameters(name = "{index}: {0}")
    public static Iterable<WriteFiles<?>> data() {
      return ImmutableList.<WriteFiles<?>>of(WriteFiles.to(new DummySink()));
    }

    @Parameter(0)
    public WriteFiles<String> writeFiles;

    @Test
    public void testToAndFromProto() throws Exception {
      RunnerApi.WriteFilesPayload payload = WriteFilesTranslation.toProto(writeFiles);

      assertThat(
          (FileBasedSink<String>) WriteFilesTranslation.fromProto(payload.getSink()),
          equalTo(writeFiles.getSink()));

      assertThat(
          payload.getRunnerDeterminedSharding(),
          equalTo(writeFiles.getNumShards() == null && writeFiles.getSharding() == null));

      assertThat(payload.getWindowedWrites(), equalTo(writeFiles.isWindowedWrites()));
    }
  }

  /**
   * A simple {@link FileBasedSink} for testing serialization/deserialization. Not mocked to avoid
   * any issues serializing mocks.
   */
  static class DummySink extends FileBasedSink<String> {
    public DummySink() {
      super(
          StaticValueProvider.of(FileSystems.matchNewResource("nowhere", false)),
          new DummyFilenamePolicy());
    }

    @Override
    public WriteOperation<String> createWriteOperation() {
      throw new UnsupportedOperationException("Should never be called.");
    }
  }

  static class DummyFilenamePolicy extends FilenamePolicy {
    @Override
    public ResourceId windowedFilename(
        ResourceId outputDirectory, WindowedContext c, String extension) {
      throw new UnsupportedOperationException("Should never be called.");
    }

    @Nullable
    @Override
    public ResourceId unwindowedFilename(ResourceId outputDirectory, Context c, String extension) {
      throw new UnsupportedOperationException("Should never be called.");
    }
  }
}
