package org.apache.beam.sdk.transforms;

import java.io.Serializable;
import org.apache.beam.sdk.values.PCollectionView;

public final class Contextful<ClosureT> implements Serializable {
  private final ClosureT closure;
  private final Requirements requirements;

  private Contextful(ClosureT closure, Requirements requirements) {
    this.closure = closure;
    this.requirements = requirements;
  }

  public ClosureT getClosure() {
    return closure;
  }

  public Requirements getRequirements() {
    return requirements;
  }

  public static <ClosureT> Contextful<ClosureT> of(ClosureT closure, Requirements requirements) {
    return new Contextful<>(closure, requirements);
  }

  public interface Fn<InputT, OutputT> extends Serializable {
    abstract class Context {
      public <T> T sideInput(PCollectionView<T> view) {
        throw new UnsupportedOperationException();
      }

      public static <InputT> Context wrapProcessContext(final DoFn<InputT, ?>.ProcessContext c) {
        return new ContextFromProcessContext<>(c);
      }

      private static class ContextFromProcessContext<InputT> extends Context {
        private final DoFn<InputT, ?>.ProcessContext c;

        ContextFromProcessContext(DoFn<InputT, ?>.ProcessContext c) {
          this.c = c;
        }

        @Override
        public <T> T sideInput(PCollectionView<T> view) {
          return c.sideInput(view);
        }
      }
    }

    OutputT apply(InputT element, Context c) throws Exception;
  }

  public static <InputT, OutputT> Contextful<Fn<InputT, OutputT>> fn(
      final SerializableFunction<InputT, OutputT> fn) {
    return new Contextful<Fn<InputT, OutputT>>(
        new Fn<InputT, OutputT>() {
          @Override
          public OutputT apply(InputT element, Context c) throws Exception {
            return fn.apply(element);
          }
        },
        Requirements.empty());
  }

  public static <InputT, OutputT> Contextful<Fn<InputT, OutputT>> fn(
      final Fn<InputT, OutputT> fn, Requirements requirements) {
    return of(fn, requirements);
  }
}
