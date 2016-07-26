package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Sum.SumIntegerFn;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link AggregatorContainer}.
 */
public class AggregatorContainerTest {

  @Rule
  public final ExpectedException thrown = ExpectedException.none();
  private final AggregatorContainer container = AggregatorContainer.create();

  @Test
  public void addsAggregatorsOnCommit() {
    AggregatorContainer.Mutator mutator = container.createMutator();
    mutator.createAggregator("sum_int", new SumIntegerFn()).addValue(5);
    mutator.commit();

    assertThat((Integer) container.getAggregate("sum_int"), equalTo(5));

    mutator = container.createMutator();
    mutator.createAggregator("sum_int", new SumIntegerFn()).addValue(8);

    assertThat("Shouldn't update value until commit",
        (Integer) container.getAggregate("sum_int"), equalTo(5));
    mutator.commit();
    assertThat((Integer) container.getAggregate("sum_int"), equalTo(13));
  }

  @Test
  public void failToCreateAfterCommit() {
    AggregatorContainer.Mutator mutator = container.createMutator();
    mutator.commit();

    thrown.expect(IllegalStateException.class);
    mutator.createAggregator("sum_int", new SumIntegerFn()).addValue(5);
  }

  @Test
  public void failToAddValueAfterCommit() {
    AggregatorContainer.Mutator mutator = container.createMutator();
    Aggregator<Integer, ?> aggregator = mutator.createAggregator("sum_int", new SumIntegerFn());
    mutator.commit();

    thrown.expect(IllegalStateException.class);
    aggregator.addValue(5);
  }

  @Test
  public void failToAddValueAfterCommitWithPrevious() {
    AggregatorContainer.Mutator mutator = container.createMutator();
    mutator.createAggregator("sum_int", new SumIntegerFn()).addValue(5);
    mutator.commit();

    mutator = container.createMutator();
    Aggregator<Integer, ?> aggregator = mutator.createAggregator("sum_int", new SumIntegerFn());
    mutator.commit();

    thrown.expect(IllegalStateException.class);
    aggregator.addValue(5);
  }
}
