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

package org.apache.beam.sdk.testing;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

/** Tests for {@link TestPipeline}. */
@RunWith(Suite.class)
@Suite.SuiteClasses({ TestPipelineTest.TestPipelineCreationTest.class,
                      TestPipelineTest.TestPipelineEnforcementsTest.class })
public class TestPipelineTest implements Serializable {

  /**
   * Tests related to the creation of a {@link TestPipeline}.
   */
  @RunWith(JUnit4.class)
  public static class TestPipelineCreationTest {
    @Rule public transient TestRule restoreSystemProperties = new RestoreSystemProperties();
    @Rule public transient ExpectedException thrown = ExpectedException.none();
    @Rule public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testCreationUsingDefaults() {
      assertNotNull(pipeline);
      assertNotNull(TestPipeline.create());
    }

    @Test
    public void testCreationNotAsTestRule() {
      thrown.expect(IllegalStateException.class);
      thrown.expectMessage("@Rule");

      TestPipeline.create().run();
    }

    @Test
    public void testCreationOfPipelineOptions() throws Exception {
      ObjectMapper mapper = new ObjectMapper();
      String stringOptions =
          mapper.writeValueAsString(
              new String[] {
                  "--runner=org.apache.beam.sdk.testing.CrashingRunner", "--project=testProject"
              });
      System.getProperties().put("beamTestPipelineOptions", stringOptions);
      GcpOptions options = TestPipeline.testingPipelineOptions().as(GcpOptions.class);
      assertEquals(CrashingRunner.class, options.getRunner());
      assertEquals(options.getProject(), "testProject");
    }

    @Test
    public void testCreationOfPipelineOptionsFromReallyVerboselyNamedTestCase() throws Exception {
      PipelineOptions options = TestPipeline.testingPipelineOptions();
      assertThat(
          options.as(ApplicationNameOptions.class).getAppName(),
          startsWith("TestPipelineTest$TestPipelineCreationTest"
                         + "-testCreationOfPipelineOptionsFromReallyVerboselyNamedTestCase"));
    }

    @Test
    public void testToString() {
      assertEquals("TestPipeline#TestPipelineTest$TestPipelineCreationTest-testToString",
                   TestPipeline.create().toString());
    }

    @Test
    public void testToStringNestedMethod() {
      TestPipeline p = nestedMethod();

      assertEquals(
          "TestPipeline#TestPipelineTest$TestPipelineCreationTest-testToStringNestedMethod",
          p.toString());
      assertEquals(
          "TestPipelineTest$TestPipelineCreationTest-testToStringNestedMethod",
          p.getOptions().as(ApplicationNameOptions.class).getAppName());
    }

    private TestPipeline nestedMethod() {
      return TestPipeline.create();
    }

    @Test
    public void testConvertToArgs() {
      String[] args = new String[] {"--tempLocation=Test_Location"};
      PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
      String[] arr = TestPipeline.convertToArgs(options);
      List<String> lst = Arrays.asList(arr);
      assertEquals(lst.size(), 2);
      assertThat(lst, containsInAnyOrder("--tempLocation=Test_Location",
                                         "--appName=TestPipelineCreationTest"));
    }

    @Test
    public void testToStringNestedClassMethod() {
      TestPipeline p = new NestedTester().p();

      assertEquals(
          "TestPipeline#TestPipelineTest$TestPipelineCreationTest-testToStringNestedClassMethod",
          p.toString());
      assertEquals(
          "TestPipelineTest$TestPipelineCreationTest-testToStringNestedClassMethod",
          p.getOptions().as(ApplicationNameOptions.class).getAppName());
    }

    private static class NestedTester {
      public TestPipeline p() {
        return TestPipeline.create();
      }
    }

    @Test
    public void testMatcherSerializationDeserialization() {
      TestPipelineOptions opts = PipelineOptionsFactory.as(TestPipelineOptions.class);
      SerializableMatcher<PipelineResult> m1 = new TestMatcher();
      SerializableMatcher<PipelineResult> m2 = new TestMatcher();

      opts.setOnCreateMatcher(m1);
      opts.setOnSuccessMatcher(m2);

      String[] arr = TestPipeline.convertToArgs(opts);
      TestPipelineOptions newOpts =
          PipelineOptionsFactory.fromArgs(arr).as(TestPipelineOptions.class);

      assertEquals(m1, newOpts.getOnCreateMatcher());
      assertEquals(m2, newOpts.getOnSuccessMatcher());
    }

    @Test
    public void testRunWithDummyEnvironmentVariableFails() {
      System.getProperties()
            .setProperty(TestPipeline.PROPERTY_USE_DEFAULT_DUMMY_RUNNER, Boolean.toString(true));
      pipeline.apply(Create.of(1, 2, 3));

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("Cannot call #run");
      pipeline.run();
    }

    /** TestMatcher is a matcher designed for testing matcher serialization/deserialization. */
    public static class TestMatcher extends BaseMatcher<PipelineResult>
        implements SerializableMatcher<PipelineResult> {
      private final UUID uuid = UUID.randomUUID();

      @Override
      public boolean matches(Object o) {
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(String.format("%tL", new Date()));
      }

      @Override
      public boolean equals(Object obj) {
        if (!(obj instanceof TestMatcher)) {
          return false;
        }
        TestMatcher other = (TestMatcher) obj;
        return other.uuid.equals(uuid);
      }

      @Override
      public int hashCode() {
        return uuid.hashCode();
      }
    }
  }

  /**
   * Tests for {@link TestPipeline}'s detection of missing {@link Pipeline#run()}, or abandoned
   * (dangling) {@link PAssert} or {@link org.apache.beam.sdk.transforms.PTransform} nodes.
   */
  @RunWith(Parameterized.class)
  public static class TestPipelineEnforcementsTest implements Serializable {

    private static final List<String> WORDS = Collections.singletonList("hi there");
    private static final String WHATEVER = "expected";
    private static final String P_TRANSFORM = "PTransform";
    private static final String P_ASSERT = "PAssert";

    private static class DummyRunner extends PipelineRunner<PipelineResult> {

      @SuppressWarnings("unused") // used by reflection
      public static DummyRunner fromOptions(final PipelineOptions opts) {
        return new DummyRunner();
      }

      @Override
      public PipelineResult run(final Pipeline pipeline) {
        return new PipelineResult() {

          @Override
          public State getState() {
            return null;
          }

          @Override
          public State cancel() throws IOException {
            return null;
          }

          @Override
          public State waitUntilFinish(final Duration duration) {
            return null;
          }

          @Override
          public State waitUntilFinish() {
            return null;
          }

          @Override
          public <T> AggregatorValues<T> getAggregatorValues(final Aggregator<?, T> aggregator)
              throws AggregatorRetrievalException {
            return null;
          }

          @Override
          public MetricResults metrics() {
            return null;
          }
        };
      }
    }

    @SuppressWarnings("UnusedReturnValue")
    private static PCollection<String> addTransform(final PCollection<String> pCollection) {
      return pCollection.apply("Map2", MapElements.via(new SimpleFunction<String, String>() {

        @Override
        public String apply(final String input) {
          return WHATEVER;
        }
      }));
    }

    private static PCollection<String> pCollection(final Pipeline pipeline) {
      return pipeline.apply("Create",
                            Create.of(WORDS).withCoder(StringUtf8Coder.of()))
                     .apply("Map1",
                            MapElements.via(new SimpleFunction<String, String>() {

                              @Override
                              public String apply(final String input) {
                                return WHATEVER;
                              }
                            }));
    }

    /**
     * Tests for {@link TestPipeline}s with a non {@link CrashingRunner}.
     */
    public static class NonCrashingRunner {

      private final transient ExpectedException exception = ExpectedException.none();

      private final transient TestPipeline pipeline = TestPipeline.fromOptions(dummyRunner());

      @Rule
      public final transient RuleChain chain = RuleChain.outerRule(exception).around(pipeline);

      private static PipelineOptions dummyRunner() {
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        pipelineOptions.setRunner(DummyRunner.class);
        return pipelineOptions;
      }

      @Category(RunnableOnService.class)
      @Test
      public void testNormalFlow() throws Exception {
        addTransform(pCollection(pipeline));
        pipeline.run();
      }

      @Category(RunnableOnService.class)
      @Test
      public void testMissingRun() throws Exception {

        exception.expect(TestPipeline.PipelineRunMissingException.class);

        addTransform(pCollection(pipeline));
      }

      @Category(RunnableOnService.class)
      @Test
      public void testMissingRunWithDisabledEnforcement() throws Exception {
        pipeline.enableAbandonedNodeEnforcement(false);
        addTransform(pCollection(pipeline));

        // disable abandoned node detection
      }

      @Category(RunnableOnService.class)
      @Test
      public void testMissingRunAutoAdd() throws Exception {
        pipeline.enableAutoRunIfMissing(true);
        addTransform(pCollection(pipeline));

        // have the pipeline.run() auto-added
      }

      @Category(RunnableOnService.class)
      @Test
      public void testDanglingPTransformRunnableOnService() throws Exception {

        exception.expect(TestPipeline.AbandonedNodeException.class);
        exception.expectMessage(P_TRANSFORM);

        final PCollection<String> pCollection = pCollection(pipeline);
        PAssert.that(pCollection).containsInAnyOrder(WHATEVER);
        pipeline.run().waitUntilFinish();

        // dangling PTransform
        addTransform(pCollection);
      }

      @Category(NeedsRunner.class)
      @Test
      public void testDanglingPTransformNeedsRunner() throws Exception {

        exception.expect(TestPipeline.AbandonedNodeException.class);
        exception.expectMessage(P_TRANSFORM);

        final PCollection<String> pCollection = pCollection(pipeline);
        PAssert.that(pCollection).containsInAnyOrder(WHATEVER);
        pipeline.run().waitUntilFinish();

        // dangling PTransform
        addTransform(pCollection);
      }

      @Category(RunnableOnService.class)
      @Test
      public void testDanglingPAssertRunnableOnService() throws Exception {

        exception.expect(TestPipeline.AbandonedNodeException.class);
        exception.expectMessage(P_ASSERT);

        final PCollection<String> pCollection = pCollection(pipeline);
        PAssert.that(pCollection).containsInAnyOrder(WHATEVER);
        pipeline.run().waitUntilFinish();

        // dangling PAssert
        PAssert.that(pCollection).containsInAnyOrder(WHATEVER);
      }

      /**
       * Tests that a {@link TestPipeline} rule behaves as expected when there is no pipeline usage
       * within a test that has a {@link RunnableOnService} annotation.
       */
      @Category(RunnableOnService.class)
      @Test
      public void testNoTestPipelineUsedRunnableOnService() { }

      /**
       * Tests that a {@link TestPipeline} rule behaves as expected when there is no pipeline usage
       * present in a test.
       */
      @Test
      public void testNoTestPipelineUsedNoAnnotation() { }

    }

    /**
     * Tests for {@link TestPipeline}s with a {@link CrashingRunner}.
     */
    public static class WithCrashingRunner {

      static {
        System.setProperty(TestPipeline.PROPERTY_USE_DEFAULT_DUMMY_RUNNER, Boolean.TRUE.toString());
      }

      private final transient ExpectedException exception = ExpectedException.none();

      private final transient TestPipeline pipeline = TestPipeline.create();

      @Rule
      public final transient RuleChain chain = RuleChain.outerRule(exception).around(pipeline);

      @Test
      public void testNoTestPipelineUsed() { }

      @Test
      public void testMissingRun() throws Exception {
        addTransform(pCollection(pipeline));

        // pipeline.run() is missing, BUT:
        // 1. Neither @RunnableOnService nor @NeedsRunner are present, AND
        // 2. The runner class is CrashingRunner.class
        // (1) + (2) => we assume this pipeline was never meant to be run, so no exception is
        // thrown on account of the missing run / dangling nodes.
      }

    }

    /**
     * Tests for {@link TestPipeline}s with a {@link CrashingRunner}.
     */
    public static class WithConflictingCrashingRunner {


      private static final transient ExpectedException exception = ExpectedException.none();

      static {
        System.setProperty(TestPipeline.PROPERTY_USE_DEFAULT_DUMMY_RUNNER, Boolean.TRUE.toString());
        exception.expect(IllegalStateException.class);
      }

      private final transient TestPipeline pipeline = TestPipeline.create();

      @Rule
      public final transient RuleChain chain = RuleChain.outerRule(exception).around(pipeline);

      @Category(NeedsRunner.class)
      @Test
      public void testConflictNeedsRunnerWithCrashingRunner() throws Exception {

        addTransform(pCollection(pipeline));

        // @NeedsRunner annotation with a CrashingRunner indicates a configuration issue
      }

      @Category(RunnableOnService.class)
      @Test
      public void testConflictRunnableOnServiceWithCrashingRunner() throws Exception {

        addTransform(pCollection(pipeline));

        // @RunnableOnService annotation with a CrashingRunner indicates a configuration issue
      }

    }

    private static List<Object[]> extractTests(final Class<?> testClass) {
      final ArrayList<org.junit.runner.Description> testDescriptions = Request.aClass(testClass)
                                                                              .getRunner()
                                                                              .getDescription()
                                                                              .getChildren();

      return FluentIterable.from(testDescriptions)
                           .transform(new Function<org.junit.runner.Description, Object[]>() {

                             @Override
                             public Object[] apply(final org.junit.runner.Description description) {
                               return new Object[] {
                                   Request.method(testClass, description.getMethodName()),
                                   description.getTestClass().getSimpleName() + "#"
                                       + description.getMethodName()
                               };
                             }
                           })
                           .toList();
    }

    private void throwOnFailures(final Result runResult) throws Throwable {
      if (!runResult.getFailures().isEmpty()) {
        throw Iterables.getFirst(runResult.getFailures(), null).getException();
      }
    }

    private void runTest(final Request test) throws Throwable {

      final JUnitCore jUnitCore = new JUnitCore();
      final Result runResult = jUnitCore.run(test);

      throwOnFailures(runResult);
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> testCombos() {
      final Iterable<Object[]> tests =
          Iterables.concat(
              extractTests(TestPipelineEnforcementsTest.NonCrashingRunner.class),
              extractTests(TestPipelineEnforcementsTest.WithCrashingRunner.class),
              extractTests(TestPipelineEnforcementsTest.WithConflictingCrashingRunner.class));

      return ImmutableList.<Object[]>builder().addAll(tests).build();
    }

    @SuppressWarnings("DefaultAnnotationParam")
    @Parameterized.Parameter(0)
    public Request testCase;

    @Parameterized.Parameter(1)
    public String testName;

    @Test
    public void run() throws Throwable {
      runTest(testCase);
    }

  }
}
