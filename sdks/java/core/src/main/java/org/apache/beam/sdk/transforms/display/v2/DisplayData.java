package org.apache.beam.sdk.transforms.display.v2;

import java.io.Serializable;
import java.util.Set;

import javax.annotation.Nullable;

/** Prototype interface for next-gen display data! */
public class DisplayData implements Serializable {

  /** Initialize display data for a component */
  public static DisplayData init(Class<?> component) { return null; }

  /** Add or update a display data item. */
  public DisplayData add(DisplayData.Item<?> item) { return this; }
  public DisplayData addIfNotNull(DisplayData.Item<?> item) { return this; }

  /** Individual display data key-value-pairs */
  public static class Item<T> {
    public String getNamespace() { return null; }
    public String getKey() { return null; }
    public T getValue() { return null; }

    public DisplayData.Item<T> withLabel(String label) { return this; }
    @Nullable public String getLabel() { return null; }

    public DisplayData.Item<T> withLinkUrl(String linkUrl) { return this; }
    @Nullable public String getLinkUrl() { return null; }
  }

  /** Create a display data item to register */
  public static <T> DisplayData.Item<T> item(String key, T value) { return null; }

  /** Include a subcomponent's display data. */
  public DisplayData include(DisplayData subComponentDisplayData) { return this; }

  /** Get a snapshot of currently registered display data. */
  public Set<DisplayData.Item<?>> snapshot() { return null; }

  /** Register callbacks when display data is updated */
  public void registerObserver(DisplayData.Observer observer) {}

  /** Observer interface to receive callbacks when display data is updated */
  public interface Observer {
    void added(DisplayData.Item<?> item);
    void updated(DisplayData.Item<?> item);
  }

  /** Empty display data; helper for no-op HasDisplayData implementation */
  public static DisplayData empty() { return null; }
}
