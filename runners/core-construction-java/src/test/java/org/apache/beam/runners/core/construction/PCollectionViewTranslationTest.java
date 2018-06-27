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

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.transforms.Materialization;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PCollectionViewTranslation}. */
@RunWith(JUnit4.class)
public class PCollectionViewTranslationTest {
  @Test
  public void testViewFnTranslation() throws Exception {
    assertEquals(
        new TestViewFn(),
        PCollectionViewTranslation.viewFnFromProto(
            ParDoTranslation.translateViewFn(new TestViewFn(), SdkComponents.create())));
  }

  @Test
  public void testWindowMappingFnTranslation() throws Exception {
    assertEquals(
        new GlobalWindows().getDefaultWindowMappingFn(),
        PCollectionViewTranslation.windowMappingFnFromProto(
            ParDoTranslation.translateWindowMappingFn(
                new GlobalWindows().getDefaultWindowMappingFn(), SdkComponents.create())));
  }

  /** Test implementation to check for equality. */
  private static class TestViewFn extends ViewFn<Object, Object> {
    @Override
    public Materialization<Object> getMaterialization() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object apply(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof TestViewFn;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }
}
