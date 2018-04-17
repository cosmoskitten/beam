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
package org.apache.beam.sdk.extensions.sql.impl;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;

/**
 * BeamEnumerable executes a beam pipeline.
 */
public class BeamEnumerable extends AbstractEnumerable<Object>
    implements Enumerable<Object> {

  /**
   * TODO: Don't use global state. This is a bad idea, but works for manual testing.
   */
  private static Queue<Object[]> values = new ConcurrentLinkedQueue<Object[]>();

  public static BeamEnumerable fromNode(BeamRelNode node) {
    Pipeline pipeline = Pipeline.create();
    BeamEnumerable enumerable = new BeamEnumerable();

    PCollectionTuple.empty(pipeline)
        .apply(node.toPTransform())
        .apply(ParDo.of(
            new DoFn<Row, Void>() {
              @ProcessElement
              public void processElement(ProcessContext context) {
                Row input = context.element();
                values.add(input.getValues().toArray());
              }
            }));
    pipeline.run().waitUntilFinish();
    return enumerable;
  }

  @Override
  public Enumerator<Object> enumerator() {
    return new BeamEnumerator();
  }

  private class BeamEnumerator implements Enumerator<Object> {

    private Iterator<Object[]> iterator;
    private Object[] current;

    BeamEnumerator() {
      iterator = values.iterator();
      current = null;
    }

    @Override
    public Object current() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      if (current.length == 1) {
        return current[0];
      }
      return current;
    }

    @Override
    public boolean moveNext() {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      }
      return false;
    }

    @Override
    public void reset() {
      iterator = values.iterator();
      current = null;
    }

    @Override
    public void close() {
      iterator = null;
      values = new ConcurrentLinkedQueue<Object[]>();
    }
  }
}
