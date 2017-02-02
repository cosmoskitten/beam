/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.spark;

import com.google.common.base.Preconditions;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.TimedWindow;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

class ReduceByKeyTranslator implements SparkOperatorTranslator<ReduceByKey> {

  static boolean wantTranslate(ReduceByKey operator) {
    boolean b = operator.isCombinable()
            && (operator.getWindowing() == null
                || (!(operator.getWindowing() instanceof MergingWindowing)
                    && !operator.getWindowing().getTrigger().isStateful()));
    return b;
  }

  @Override
  public JavaRDD<?> translate(ReduceByKey operator, SparkExecutorContext context) {
    final JavaRDD<WindowedElement> input = (JavaRDD) context.getSingleInput(operator);

    final UnaryFunction<Iterable, Object> reducer = operator.getReducer();
    final Partitioning partitioning = operator.getPartitioning();
    final UnaryFunction<?, Long> eventTimeAssigner = operator.getEventTimeAssigner();
    final Windowing windowing =
            operator.getWindowing() == null
                    ? AttachedWindowing.INSTANCE
                    : operator.getWindowing();
    final UnaryFunction keyExtractor = operator.getKeyExtractor();
    final UnaryFunction valueExtractor = operator.getValueExtractor();

    Preconditions.checkState(operator.isCombinable(),
            "Non-combinable ReduceByKey not supported!");
    Preconditions.checkState(
            !(windowing instanceof MergingWindowing),
            "MergingWindowing not supported!");
    Preconditions.checkState(!windowing.getTrigger().isStateful(),
            "Stateful triggers not supported!");

    // ~ extract key/value + timestamp from input elements and assign windows
    JavaPairRDD<KeyedWindow, TimestampedElement> tuples = input.flatMapToPair(
            new CompositeKeyExtractor(
                    keyExtractor, valueExtractor, windowing, eventTimeAssigner));

    JavaPairRDD<KeyedWindow, TimestampedElement> reduced;
    if (partitioning.hasDefaultPartitioner()) {
      reduced = tuples.reduceByKey(new Reducer(reducer), partitioning.getNumPartitions());
    } else {
      reduced = tuples.reduceByKey(new PartitioningWrapper(partitioning), new Reducer(reducer));
    }

    return reduced.map(t -> {
      KeyedWindow kw = t._1();
      TimestampedElement el = t._2();

      // ~ extract timestamp from element rather than from KeyedWindow
      // because in KeyedWindow there is the original timestamp from
      // pre-reduce age
      long timestamp = el.getTimestamp();

      return new WindowedElement<>(kw.window(), timestamp,
              Pair.of(kw.key(), el.getElement()));
    });
  }

  /**
   * Extracts {@link KeyedWindow} from {@link WindowedElement} and
   * assigns timestamp according to (optional) eventTimeAssigner.
   */
  private static class CompositeKeyExtractor
          implements PairFlatMapFunction<WindowedElement, KeyedWindow, TimestampedElement> {

    private final UnaryFunction keyExtractor;
    private final UnaryFunction valueExtractor;
    private final Windowing windowing;
    private final UnaryFunction eventTimeAssigner;

    public CompositeKeyExtractor(UnaryFunction keyExtractor,
                                 UnaryFunction valueExtractor,
                                 Windowing windowing,
                                 UnaryFunction<?, Long> eventTimeAssigner /* optional */) {
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
      this.windowing = windowing;
      this.eventTimeAssigner = eventTimeAssigner;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Tuple2<KeyedWindow, TimestampedElement>> call(WindowedElement wel) throws Exception {
      if (eventTimeAssigner != null) {
        wel.setTimestamp((long) eventTimeAssigner.apply(wel.getElement()));
      }

      Set<Window> windows = windowing.assignWindowsToElement(wel);
      List<Tuple2<KeyedWindow, TimestampedElement>> out = new ArrayList<>(windows.size());
      for (Window wid : windows) {
        Object el = wel.getElement();
        long stamp = (wid instanceof TimedWindow)
                ? ((TimedWindow) wid).maxTimestamp()
                : wel.getTimestamp();
        out.add(new Tuple2<>(
                new KeyedWindow<>(wid, stamp, keyExtractor.apply(el)),
                new TimestampedElement(stamp, valueExtractor.apply(el))));
      }
      return out.iterator();
    }
  }


  private static class Reducer implements
          Function2<TimestampedElement, TimestampedElement, TimestampedElement> {

    private final UnaryFunction<Iterable, Object> reducer;

    // cached array to avoid repeated allocation
    private Object[] iterable;

    private Reducer(UnaryFunction<Iterable, Object> reducer) {
      this.reducer = reducer;
      this.iterable = new Object[2];
    }

    @Override
    public TimestampedElement call(TimestampedElement o1, TimestampedElement o2) {
      iterable[0] = o1.getElement();
      iterable[1] = o2.getElement();
      Object result = reducer.apply(Arrays.asList(iterable));

      return new TimestampedElement(
              Math.max(o1.getTimestamp(), o2.getTimestamp()), result);
    }
  }
}
