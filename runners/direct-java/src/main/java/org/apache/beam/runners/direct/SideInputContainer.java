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
package org.apache.beam.runners.direct;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.PCollectionViewWindow;
import org.apache.beam.sdk.util.ReadyCheckingSideInputReader;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * An in-process container for {@link PCollectionView PCollectionViews}, which provides methods for
 * constructing {@link SideInputReader SideInputReaders} which block until a side input is
 * available and writing to a {@link PCollectionView}.
 */
class SideInputContainer {
  private final Collection<PCollectionView<?>> containedViews;
  private final LoadingCache<
          PCollectionViewWindow<?>, AtomicReference<Iterable<? extends WindowedValue<?>>>>
      viewByWindows;

  /**
   * Create a new {@link SideInputContainer} with the provided views and the provided
   * context.
   */
  public static SideInputContainer create(
      final EvaluationContext context, Collection<PCollectionView<?>> containedViews) {
    LoadingCache<PCollectionViewWindow<?>, AtomicReference<Iterable<? extends WindowedValue<?>>>>
        viewByWindows = CacheBuilder.newBuilder().build(new CallbackSchedulingLoader(context));
    return new SideInputContainer(containedViews, viewByWindows);
  }

  private SideInputContainer(
      Collection<PCollectionView<?>> containedViews,
      LoadingCache<PCollectionViewWindow<?>, AtomicReference<Iterable<? extends WindowedValue<?>>>>
          viewByWindows) {
    this.containedViews = ImmutableSet.copyOf(containedViews);
    this.viewByWindows = viewByWindows;
  }

  /**
   * Return a view of this {@link SideInputContainer} that contains only the views in the
   * provided argument. The returned {@link SideInputContainer} is unmodifiable without
   * casting, but will change as this {@link SideInputContainer} is modified.
   */
  public ReadyCheckingSideInputReader createReaderForViews(
      Collection<PCollectionView<?>> newContainedViews) {
    if (!containedViews.containsAll(newContainedViews)) {
      Set<PCollectionView<?>> currentlyContained = ImmutableSet.copyOf(containedViews);
      Set<PCollectionView<?>> newRequested = ImmutableSet.copyOf(newContainedViews);
      throw new IllegalArgumentException("Can't create a SideInputReader with unknown views "
          + Sets.difference(newRequested, currentlyContained));
    }
    return new SideInputContainerSideInputReader(newContainedViews);
  }

  /**
   * Write the provided values to the provided view.
   *
   * <p>The windowed values are first exploded, then for each window the pane is determined. For
   * each window, if the pane is later than the current pane stored within this container, write
   * all of the values to the container as the new values of the {@link PCollectionView}.
   *
   * <p>The provided iterable is expected to contain only a single window and pane.
   */
  public void write(PCollectionView<?> view, Iterable<? extends WindowedValue<?>> values) {
    Map<BoundedWindow, Collection<WindowedValue<?>>> valuesPerWindow =
        indexValuesByWindow(values);
    for (Map.Entry<BoundedWindow, Collection<WindowedValue<?>>> windowValues :
        valuesPerWindow.entrySet()) {
      updatePCollectionViewWindowValues(view, windowValues.getKey(), windowValues.getValue());
    }
  }

  /**
   * Index the provided values by all {@link BoundedWindow windows} in which they appear.
   */
  private Map<BoundedWindow, Collection<WindowedValue<?>>> indexValuesByWindow(
      Iterable<? extends WindowedValue<?>> values) {
    Map<BoundedWindow, Collection<WindowedValue<?>>> valuesPerWindow = new HashMap<>();
    for (WindowedValue<?> value : values) {
      for (BoundedWindow window : value.getWindows()) {
        Collection<WindowedValue<?>> windowValues = valuesPerWindow.get(window);
        if (windowValues == null) {
          windowValues = new ArrayList<>();
          valuesPerWindow.put(window, windowValues);
        }
        windowValues.add(value);
      }
    }
    return valuesPerWindow;
  }

  /**
   * Set the value of the {@link PCollectionView} in the {@link BoundedWindow} to be based on the
   * specified values, if the values are part of a later pane than currently exist within the
   * {@link PCollectionViewWindow}.
   */
  private void updatePCollectionViewWindowValues(
      PCollectionView<?> view, BoundedWindow window, Collection<WindowedValue<?>> windowValues) {
    PCollectionViewWindow<?> windowedView = PCollectionViewWindow.of(view, window);
    AtomicReference<Iterable<? extends WindowedValue<?>>> contents =
        viewByWindows.getUnchecked(windowedView);
    if (contents.compareAndSet(null, windowValues)) {
      // the value had never been set, so we set it and are done.
      return;
    }
    PaneInfo newPane = windowValues.iterator().next().getPane();

    Iterable<? extends WindowedValue<?>> existingValues;
    long existingPane;
    do {
      existingValues = contents.get();
      existingPane =
          Iterables.isEmpty(existingValues)
              ? -1L
              : existingValues.iterator().next().getPane().getIndex();
    } while (newPane.getIndex() > existingPane
        && !contents.compareAndSet(existingValues, windowValues));
  }

  private static class CallbackSchedulingLoader extends
      CacheLoader<PCollectionViewWindow<?>, AtomicReference<Iterable<? extends WindowedValue<?>>>> {
    private final EvaluationContext context;

    public CallbackSchedulingLoader(
        EvaluationContext context) {
      this.context = context;
    }

    @Override
    public AtomicReference<Iterable<? extends WindowedValue<?>>>
        load(PCollectionViewWindow<?> view) {

      AtomicReference<Iterable<? extends WindowedValue<?>>> contents = new AtomicReference<>();
      WindowingStrategy<?, ?> windowingStrategy = view.getView().getWindowingStrategyInternal();

      context.scheduleAfterOutputWouldBeProduced(view.getView(),
          view.getWindow(),
          windowingStrategy,
          new WriteEmptyViewContents(view.getView(), view.getWindow(), contents));
      return contents;
    }
  }

  private static class WriteEmptyViewContents implements Runnable {
    private final PCollectionView<?> view;
    private final BoundedWindow window;
    private final AtomicReference<Iterable<? extends WindowedValue<?>>> contents;

    private WriteEmptyViewContents(PCollectionView<?> view, BoundedWindow window,
        AtomicReference<Iterable<? extends WindowedValue<?>>> contents) {
      this.contents = contents;
      this.view = view;
      this.window = window;
    }

    @Override
    public void run() {
      // The requested window has closed without producing elements, so reflect that in
      // the PCollectionView. If set has already been called, will do nothing.
      contents.compareAndSet(null, Collections.<WindowedValue<?>>emptyList());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("view", view)
          .add("window", window)
          .toString();
    }
  }

  private final class SideInputContainerSideInputReader implements ReadyCheckingSideInputReader {
    private final Collection<PCollectionView<?>> readerViews;
    private final LoadingCache<
        PCollectionViewWindow<?>, Optional<? extends Iterable<? extends WindowedValue<?>>>>
        viewContents;

    private SideInputContainerSideInputReader(Collection<PCollectionView<?>> readerViews) {
      this.readerViews = ImmutableSet.copyOf(readerViews);
      this.viewContents = CacheBuilder.newBuilder().build(new CurrentViewContentsLoader());
    }

    @Override
    public boolean isReady(final PCollectionView<?> view, final BoundedWindow window) {
      checkArgument(
          readerViews.contains(view),
          "Tried to check if view %s was ready in a SideInputReader that does not contain it. "
              + "Contained views; %s",
          view,
          readerViews);
      return viewContents.getUnchecked(PCollectionViewWindow.of(view, window)).isPresent();
    }

    @Override
    @Nullable
    public <T> T get(final PCollectionView<T> view, final BoundedWindow window) {
      checkArgument(readerViews.contains(view),
          "call to get(PCollectionView) with unknown view: %s",
          view);
      checkArgument(
          isReady(view, window),
          "calling get() on PCollectionView %s that is not ready in window %s",
          view,
          window);
      // Safe covariant cast
      @SuppressWarnings("unchecked") Iterable<WindowedValue<?>> values =
          (Iterable<WindowedValue<?>>) viewContents.getUnchecked(PCollectionViewWindow.of(view,
              window)).get();
      return view.getViewFn().apply(values);
    }

    @Override
    public <T> boolean contains(PCollectionView<T> view) {
      return readerViews.contains(view);
    }

    @Override
    public boolean isEmpty() {
      return readerViews.isEmpty();
    }
  }

  /**
   * A {@link CacheLoader} that loads the current contents of a {@link PCollectionViewWindow} into
   * an optional.
   */
  private class CurrentViewContentsLoader extends CacheLoader<
      PCollectionViewWindow<?>, Optional<? extends Iterable<? extends WindowedValue<?>>>> {

    @Override
    public Optional<? extends Iterable<? extends WindowedValue<?>>>
        load(PCollectionViewWindow<?> key) {
      return Optional.fromNullable(viewByWindows.getUnchecked(key).get());
    }
  }
}
