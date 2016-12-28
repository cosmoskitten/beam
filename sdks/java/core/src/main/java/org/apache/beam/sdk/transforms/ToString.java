package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link PTransform} that converts a {@link PCollection} of type {@code T} to a
 * {@link PCollection} of type {@code String}.
 */
public class ToString<T> extends PTransform<PCollection<T>, PCollection<String>> {

  /**
   * Returns a {@link ToString} transform.
   *
   * @param <T> the type of the input {@code PCollection}.
   */
  public static <T> ToString<T> create() {
    return new ToString<>();
  }

  private ToString() {
  }

  @Override
  public PCollection<String> expand(PCollection<T> input) {
    return input.apply(MapElements.via(new ToStringFunction<T>()));
  }

  private static class ToStringFunction<T> extends SimpleFunction<T, String> {
    @Override
    public String apply(T input) {
      return input == null ? null : input.toString();
    }
  }
}
