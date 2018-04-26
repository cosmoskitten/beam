package org.apache.beam.runners.samza.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.samza.TestSamzaRunner;
import org.apache.beam.runners.samza.state.CloseableIterator;
import org.apache.beam.runners.samza.state.SamzaMapState;
import org.apache.beam.runners.samza.state.SamzaSetState;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * Tests for SamzaStoreStateInternals.
 */
public class SamzaStoreStateInternalsTest {
  public final TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testMapStateIterator() {
    final String stateId = "foo";
    final String countStateId = "count";

    DoFn<KV<String, KV<String, Integer>>, KV<String, Integer>> fn =
        new DoFn<KV<String, KV<String, Integer>>, KV<String, Integer>>() {

          @StateId(stateId)
          private final StateSpec<MapState<String, Integer>> mapState =
              StateSpecs.map(StringUtf8Coder.of(), VarIntCoder.of());
          @StateId(countStateId)
          private final StateSpec<CombiningState<Integer, int[], Integer>>
              countState = StateSpecs.combiningFromInputInternal(VarIntCoder.of(),
              Sum.ofIntegers());

          @ProcessElement
          public void processElement(
              ProcessContext c, @StateId(stateId) MapState<String, Integer> mapState,
              @StateId(countStateId) CombiningState<Integer, int[], Integer>
                  count) {
            SamzaMapState<String, Integer> state = (SamzaMapState<String, Integer>) mapState;
            KV<String, Integer> value = c.element().getValue();
            state.put(value.getKey(), value.getValue());
            count.add(1);
            if (count.read() >= 4) {
              final List<KV<String, Integer>> content = new ArrayList<>();
              try (CloseableIterator<Map.Entry<String, Integer>> iterator = state.iterator()) {
                while (iterator.hasNext()) {
                  Map.Entry<String, Integer> entry = iterator.next();
                  content.add(KV.of(entry.getKey(), entry.getValue()));
                  c.output(KV.of(entry.getKey(), entry.getValue()));
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }

              assertEquals(content,
                  ImmutableList.of(KV.of("a", 97), KV.of("b", 42), KV.of("c", 12)));
            }
          }
        };

    PCollection<KV<String, Integer>> output =
        pipeline.apply(
            Create.of(KV.of("hello", KV.of("a", 97)), KV.of("hello", KV.of("b", 42)),
                KV.of("hello", KV.of("b", 42)), KV.of("hello", KV.of("c", 12))))
            .apply(ParDo.of(fn));

    PAssert.that(output).containsInAnyOrder(KV.of("a", 97), KV.of("b", 42), KV.of("c", 12));

    TestSamzaRunner.fromOptions(
        PipelineOptionsFactory.fromArgs("--runner=org.apache.beam.runners.samza.TestSamzaRunner")
            .create()).run(pipeline);
  }

  @Test
  public void testSetStateIterator() {
    final String stateId = "foo";
    final String countStateId = "count";

    DoFn<KV<String, Integer>, Set<Integer>> fn =
        new DoFn<KV<String, Integer>, Set<Integer>>() {

          @StateId(stateId)
          private final StateSpec<SetState<Integer>> setState =
              StateSpecs.set(VarIntCoder.of());
          @StateId(countStateId)
          private final StateSpec<CombiningState<Integer, int[], Integer>>
              countState = StateSpecs.combiningFromInputInternal(VarIntCoder.of(),
              Sum.ofIntegers());

          @ProcessElement
          public void processElement(
              ProcessContext c,
              @StateId(stateId) SetState<Integer> setState,
              @StateId(countStateId) CombiningState<Integer, int[], Integer>
                  count) {
            SamzaSetState<Integer> state = (SamzaSetState<Integer>) setState;
            ReadableState<Boolean> isEmpty = state.isEmpty();
            state.add(c.element().getValue());
            assertFalse(isEmpty.read());
            count.add(1);
            if (count.read() >= 4) {
              final Set<Integer> content = new HashSet<>();
              try (CloseableIterator<Integer> iterator = state.iterator()) {
                while (iterator.hasNext()) {
                  Integer value = iterator.next();
                  content.add(value);
                }
                c.output(content);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }

              assertEquals(content, Sets.newHashSet(97, 42, 12));
            }
          }
        };

    PCollection<Set<Integer>> output =
        pipeline.apply(
            Create.of(
                KV.of("hello", 97), KV.of("hello", 42), KV.of("hello", 42), KV.of("hello", 12)))
            .apply(ParDo.of(fn));

    PAssert.that(output).containsInAnyOrder(Sets.newHashSet(97, 42, 12));

    TestSamzaRunner.fromOptions(
        PipelineOptionsFactory.fromArgs("--runner=org.apache.beam.runners.samza.TestSamzaRunner")
            .create()).run(pipeline);
  }
}
