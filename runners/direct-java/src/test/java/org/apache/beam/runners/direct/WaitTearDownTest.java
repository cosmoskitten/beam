/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.direct;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Ensure teardown is called before waituntilfinish exists.
 */
@RunWith(JUnit4.class)
public class WaitTearDownTest implements Serializable {

    private static final Map<String, Context> TEAR_DOWN_CALLED = new HashMap<>();

    @Rule
    public transient TestPipeline p = TestPipeline.create();

    @Rule
    public transient TestName testName = new TestName();

    @Before
    public void before() {
      TEAR_DOWN_CALLED.put(testName.getMethodName(), new Context());
    }

    @After
    public void after() {
      TEAR_DOWN_CALLED.remove(testName.getMethodName());
    }

    @Test
    public void tearDownRespected() {
        p.apply(Create.of("a"))
         .apply(ParDo.of(new DoFn<String, String>() {
           private String name = testName.getMethodName();

            @ProcessElement
            public void onElement(final ProcessContext ctx) {
                // no-op
            }

            @Teardown
            public void teardown() {
              final Context context = TEAR_DOWN_CALLED.get(name);
              assertNotNull(context);
              try {
                Thread.sleep(5000);
              } catch (final InterruptedException e) {
                fail(e.getMessage());
              }
              context.teardown = true;
            }
          }));
      final PipelineResult pipelineResult = p.run();
      pipelineResult.waitUntilFinish();
      final Context context = TEAR_DOWN_CALLED.get(testName.getMethodName());
      assertTrue(context.teardown);
    }

  /**
   * Variable holder for each test instance.
   */
  public static class Context {
    private volatile boolean teardown;
  }
}
