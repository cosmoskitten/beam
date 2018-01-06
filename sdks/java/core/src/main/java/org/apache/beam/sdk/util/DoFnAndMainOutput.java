package org.apache.beam.sdk.util;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

/**
 * The data that the Java SDK harness needs to execute a DoFn.
 */
@AutoValue
public abstract class DoFnAndMainOutput implements Serializable {
  public static DoFnAndMainOutput of(DoFn<?, ?> fn, TupleTag<?> tag) {
    return new AutoValue_DoFnAndMainOutput(fn, tag);
  }

  public abstract DoFn<?, ?> getDoFn();

  public abstract TupleTag<?> getMainOutputTag();
}
