package org.apache.beam.sdk.transforms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ToString} transform.
 */
@RunWith(JUnit4.class)
public class ToStringTest {
  @Rule
  public final TestPipeline p = TestPipeline.create();

  @Test
  @Category(RunnableOnService.class)
  public void testToString() {
    Integer[] ints = {1, 2, 3, 4, 5};
    PCollection<Integer> input = p.apply(Create.of(Arrays.asList(ints)));

    PCollection<String> output = input.apply(ToString.<Integer>create());

    PAssert.that(output).containsInAnyOrder(toStringList(ints));
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testToStringNullValues() {
    Integer[] ints = {1, 2, null, null, 5};
    PCollection<Integer> input = p.apply(Create.of(Arrays.asList(ints)));

    PCollection<String> output = input.apply(ToString.<Integer>create());

    PAssert.that(output).containsInAnyOrder(toStringList(ints));
    p.run();
  }

  private List<String> toStringList(Object[] ints) {
    List<String> ll = new ArrayList<>(ints.length);
    for (Object i : ints) {
      ll.add(i == null ? null : i.toString());
    }
    return ll;
  }
}
