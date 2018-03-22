package org.apache.beam.runners.flink.streaming;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

class StreamRecordStripper {

  @SuppressWarnings("Guava")
  static <T> Iterable<WindowedValue<T>> stripStreamRecordFromWindowedValue(Iterable<Object> input) {
    return FluentIterable.from(input)
        .filter(
            o ->
                o instanceof StreamRecord && ((StreamRecord) o).getValue() instanceof WindowedValue)
        .transform(
            new Function<Object, WindowedValue<T>>() {
              @Nullable
              @Override
              @SuppressWarnings({"unchecked", "rawtypes"})
              public WindowedValue<T> apply(@Nullable Object o) {
                if (o instanceof StreamRecord
                    && ((StreamRecord) o).getValue() instanceof WindowedValue) {
                  return (WindowedValue) ((StreamRecord) o).getValue();
                }
                throw new RuntimeException("unreachable");
              }
            });
  }

}
