package org.apache.beam.runners.spark.io;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.beam.runners.spark.translation.WindowingHelpers;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test for {@link SourceRDD.Bounded.ReaderToIteratorAdapter}.
 */
public class ReaderToIteratorAdapterTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private static class TestReader extends Source.Reader<Integer> {

    private Integer current = 0;
    private boolean closed = false;

    @Override
    public boolean start() throws IOException {
      return !closed;
    }

    @Override
    public boolean advance() throws IOException {
      return !closed && (++current > 0);
    }

    @Override
    public Integer getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return Instant.now();
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }

    @Override
    public Source<Integer> getCurrentSource() {
      return null;
    }
  }

  private final TestReader testReader = new TestReader();

  private final SourceRDD.Bounded.ReaderToIteratorAdapter<Integer> readerIterator =
      new SourceRDD.Bounded.ReaderToIteratorAdapter<>(new MetricsContainer(""), testReader);

  private final Iterable<WindowedValue<Integer>> readerIterable =
      new Iterable<WindowedValue<Integer>>() {

        @Override
        public Iterator<WindowedValue<Integer>> iterator() {
          return readerIterator;
        }
      };

  private final FluentIterable<Integer> integers =
      FluentIterable
          .from(readerIterable)
          .transform(WindowingHelpers.<Integer>unwindowValueFunction());

  @Test
  public void testNormalIteration() throws Exception {
    final ImmutableList<Integer> zeroToFour = integers.limit(5).toList();
    assertThat(zeroToFour, is(ImmutableList.of(0, 1, 2, 3, 4)));
  }

  @Test
  public void testUnderlyingReaderClosedAfterReadingStarted() throws Exception {
    final ImmutableList<Integer> zeroToTwo = integers.limit(3).toList();
    assertThat(zeroToTwo, is(ImmutableList.of(0, 1, 2)));

    testReader.close();
    assertThat(readerIterator.hasNext(), is(false));
    assertThat(readerIterator.hasNext(), is(false));

    exception.expect(NoSuchElementException.class);
    readerIterator.next();
  }

  @Test
  public void testUnderlyingReaderClosedBeforeReadingStarted() throws Exception {
    testReader.close();
    assertThat(readerIterator.hasNext(), is(false));
    assertThat(readerIterator.hasNext(), is(false));

    exception.expect(NoSuchElementException.class);
    readerIterator.next();
  }

  @Test
  public void testHasNextIdempotency() throws Exception {
    assertThat(readerIterator.next().getValue(), is(0));
    assertThat(readerIterator.hasNext(), is(true));
    assertThat(readerIterator.hasNext(), is(true));
    assertThat(readerIterator.hasNext(), is(true));
    assertThat(readerIterator.hasNext(), is(true));

    assertThat(readerIterator.next().getValue(), is(1));
    assertThat(readerIterator.next().getValue(), is(2));
    assertThat(readerIterator.next().getValue(), is(3));

    assertThat(readerIterator.hasNext(), is(true));
    assertThat(readerIterator.hasNext(), is(true));
    assertThat(readerIterator.next().getValue(), is(4));
  }

}
