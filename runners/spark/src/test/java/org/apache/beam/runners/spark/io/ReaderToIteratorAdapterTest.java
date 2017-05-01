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

package org.apache.beam.runners.spark.io;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import com.google.common.base.Preconditions;
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
    private final int limit = 4;
    private boolean drained = false;

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      Preconditions.checkState(!drained && !closed);
      drained = ++current >= limit;
      return !drained;
    }

    @Override
    public Integer getCurrent() throws NoSuchElementException {
      Preconditions.checkState(!drained && !closed);
      return current;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      Preconditions.checkState(!drained && !closed);
      return Instant.now();
    }

    @Override
    public void close() throws IOException {
      Preconditions.checkState(!closed);
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
    final ImmutableList<Integer> zeroToFour = integers.limit(3).toList();
    assertThat(zeroToFour, is(ImmutableList.of(1, 2, 3)));
  }

  @Test
  public void testNextWhenDrainedThrows() throws Exception {
    final ImmutableList<Integer> zeroToFour = integers.limit(3).toList();
    assertThat(zeroToFour, is(ImmutableList.of(1, 2, 3)));

    exception.expect(NoSuchElementException.class);
    readerIterator.next();
  }

  @Test
  public void testHasNextIdempotencyCombo() throws Exception {
    assertThat(readerIterator.hasNext(), is(true));
    assertThat(readerIterator.hasNext(), is(true));

    assertThat(readerIterator.next().getValue(), is(1));

    assertThat(readerIterator.hasNext(), is(true));
    assertThat(readerIterator.hasNext(), is(true));
    assertThat(readerIterator.hasNext(), is(true));

    assertThat(readerIterator.next().getValue(), is(2));
    assertThat(readerIterator.next().getValue(), is(3));

    // drained

    assertThat(readerIterator.hasNext(), is(false));
    assertThat(readerIterator.hasNext(), is(false));

    // no next to give

    exception.expect(NoSuchElementException.class);
    readerIterator.next();
  }

}
