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
package org.apache.beam.sdk.transforms;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.WithExceptions.ExceptionAsMapHandler;
import org.apache.beam.sdk.transforms.WithExceptions.Result;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WithExceptions}. */
@RunWith(JUnit4.class)
public class WithExceptionsTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Test of {@link WithExceptions.Result#errorsTo(List)}. */
  @Test
  @Category(NeedsRunner.class)
  public void testExceptionAsMap() {
    List<PCollection<KV<Integer, Map<String, String>>>> errorCollections = new ArrayList<>();
    PCollection<Integer> output = pipeline
        .apply(Create.of(0, 1))
        .apply(
            MapElements.into(TypeDescriptors.integers())
                .via((Integer i) -> 1 / i)
                .withExceptions()
                .via(new ExceptionAsMapHandler<Integer>() {
                }))
        .errorsTo(errorCollections);

    PAssert.that(output).containsInAnyOrder(1);

    Map<String, String> expectedFailureInfo =
        ImmutableMap.of("className", "java.lang.ArithmeticException");
    PAssert.thatSingleton(PCollectionList.of(errorCollections).apply(Flatten.pCollections()))
        .satisfies(
            kv -> {
              assertEquals(Integer.valueOf(0), kv.getKey());
              assertThat(kv.getValue().entrySet(), hasSize(3));
              assertThat(kv.getValue(), hasKey("stackTrace"));
              assertEquals("java.lang.ArithmeticException", kv.getValue().get("className"));
              assertEquals("/ by zero", kv.getValue().get("message"));
              return null;
            });

    pipeline.run();
  }
}
