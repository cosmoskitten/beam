package org.apache.beam.sdk.util.state;

/**
 * State containing no duplicate elements.
 * Items can be added to the set and the contents read out.
 *
 * @param <T> The type of elements in the set.
 */
public interface SetState<T> extends CombiningState<T, Iterable<T>> {
  /**
   * Returns true if this set contains the specified element.
   */
  boolean contains(T t);

  /**
   * Add a value to the buffer if it is not already present.
   * If this set already contains the element, the call leaves the set
   * unchanged and returns false.
   */
  boolean addIfAbsent(T t);

  /**
   * Indicate that elements will be read later.
   * @param elements to be read later
   * @return this for convenient chaining
   */
  SetState<T> readLater(Iterable<T> elements);

  /**
   * <p>Checks if SetState contains any given elements.</p>
   *
   * @param elements the elements to search for
   * @return the {@code true} if any of the elements are found,
   * {@code false} if no match
   */
  boolean containsAny(Iterable<T> elements);

  /**
   * <p>Checks if SetState contains all given elements.</p>
   *
   * @param elements the elements to find
   * @return true if the SetState contains all elements,
   *  false if not
   */
  boolean containsAll(Iterable<T> elements);

  @Override
  SetState<T> readLater();
}
