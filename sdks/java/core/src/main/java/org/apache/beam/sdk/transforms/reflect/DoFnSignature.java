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
package org.apache.beam.sdk.transforms.reflect;

import com.google.auto.value.AutoValue;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.InputProvider;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.state.State;
import org.apache.beam.sdk.util.state.StateSpec;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Describes the signature of a {@link DoFn}, in particular, which features it uses, which extra
 * context it requires, types of the input and output elements, etc.
 *
 * <p>See <a href="https://s.apache.org/a-new-dofn">A new DoFn</a>.
 */
@AutoValue
public abstract class DoFnSignature {
  /** Class of the original {@link DoFn} from which this signature was produced. */
  public abstract Class<? extends DoFn<?, ?>> fnClass();

  /** Whether this {@link DoFn} does a bounded amount of work per element. */
  public abstract PCollection.IsBounded isBoundedPerElement();

  /** Details about this {@link DoFn}'s {@link DoFn.ProcessElement} method. */
  public abstract ProcessElementMethod processElement();

  /** Details about the state cells that this {@link DoFn} declares. Immutable. */
  public abstract Map<String, StateDeclaration> stateDeclarations();

  /** Details about this {@link DoFn}'s {@link DoFn.StartBundle} method. */
  @Nullable
  public abstract BundleMethod startBundle();

  /** Details about this {@link DoFn}'s {@link DoFn.FinishBundle} method. */
  @Nullable
  public abstract BundleMethod finishBundle();

  /** Details about this {@link DoFn}'s {@link DoFn.Setup} method. */
  @Nullable
  public abstract LifecycleMethod setup();

  /** Details about this {@link DoFn}'s {@link DoFn.Teardown} method. */
  @Nullable
  public abstract LifecycleMethod teardown();

  /** Details about this {@link DoFn}'s {@link DoFn.GetInitialRestriction} method. */
  @Nullable
  public abstract GetInitialRestrictionMethod getInitialRestriction();

  /** Details about this {@link DoFn}'s {@link DoFn.SplitRestriction} method. */
  @Nullable
  public abstract SplitRestrictionMethod splitRestriction();

  /** Details about this {@link DoFn}'s {@link DoFn.GetRestrictionCoder} method. */
  @Nullable
  public abstract GetRestrictionCoderMethod getRestrictionCoder();

  /** Details about this {@link DoFn}'s {@link DoFn.NewTracker} method. */
  @Nullable
  public abstract NewTrackerMethod newTracker();

  static Builder builder() {
    return new AutoValue_DoFnSignature.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setFnClass(Class<? extends DoFn<?, ?>> fnClass);
    abstract Builder setIsBoundedPerElement(PCollection.IsBounded isBounded);
    abstract Builder setProcessElement(ProcessElementMethod processElement);
    abstract Builder setStartBundle(BundleMethod startBundle);
    abstract Builder setFinishBundle(BundleMethod finishBundle);
    abstract Builder setSetup(LifecycleMethod setup);
    abstract Builder setTeardown(LifecycleMethod teardown);
    abstract Builder setGetInitialRestriction(GetInitialRestrictionMethod getInitialRestriction);
    abstract Builder setSplitRestriction(SplitRestrictionMethod splitRestriction);
    abstract Builder setGetRestrictionCoder(GetRestrictionCoderMethod getRestrictionCoder);
    abstract Builder setNewTracker(NewTrackerMethod newTracker);
    abstract Builder setStateDeclarations(Map<String, StateDeclaration> stateDeclarations);
    abstract DoFnSignature build();
  }

  /** A method delegated to a annotated method of an underlying {@link DoFn}. */
  public interface DoFnMethod {
    /** The annotated method itself. */
    Method targetMethod();
  }

  /** A descriptor for an optional parameter of the {@link DoFn.ProcessElement} method. */
  public abstract static class Parameter {

    // Private as no extensions other than those nested here are permitted
    private Parameter() {}

    /**
     * Performs case analysis on this {@link Parameter}, processing it with the appropriate
     * {@link Cases#dispatch} case of the provided {@link Cases} object.
     */
    public <ResultT> ResultT match(Cases<ResultT> cases) {
      // This could be done with reflection, but this will be in a hot loop.
      // It could also be done with bytecode generation at some point, but that
      // incurs a greater maintenance burden that may be justified.
      // Since the number of cases is small and known, they are simply inlined.
      if (this instanceof BoundedWindowParameter) {
        return cases.dispatch((BoundedWindowParameter) this);
      } else if (this instanceof RestrictionTrackerParameter) {
        return cases.dispatch((RestrictionTrackerParameter) this);
      } else if (this instanceof InputProviderParameter) {
        return cases.dispatch((InputProviderParameter) this);
      }
      if (this instanceof OutputReceiverParameter) {
        return cases.dispatch((OutputReceiverParameter) this);
      }
      if (this instanceof StateParameter) {
        return cases.dispatch((StateParameter) this);
      } else {
        throw new IllegalStateException(
            String.format("Attempt to case match on unknown %s subclass %s",
                Parameter.class.getCanonicalName(), this.getClass().getCanonicalName()));
      }
    }

    /**
     * A visitor for destructuring a {@link Parameter}.
     */
    public interface Cases<ResultT> {
      ResultT dispatch(BoundedWindowParameter p);
      ResultT dispatch(InputProviderParameter p);
      ResultT dispatch(OutputReceiverParameter p);
      ResultT dispatch(RestrictionTrackerParameter p);
      ResultT dispatch(StateParameter p);

      /**
       * A base class for a visitor with a default method for cases it is not interested in.
       */
      public abstract static class WithDefault<ResultT> implements Cases<ResultT> {

        protected abstract ResultT visitDefault(Parameter p);

        @Override
        public ResultT dispatch(BoundedWindowParameter p) {
          return visitDefault(p);
        }

        @Override
        public ResultT dispatch(InputProviderParameter p) {
          return visitDefault(p);
        }

        @Override
        public ResultT dispatch(OutputReceiverParameter p) {
          return visitDefault(p);
        }

        @Override
        public ResultT dispatch(RestrictionTrackerParameter p) {
          return visitDefault(p);
        }

        @Override
        public ResultT dispatch(StateParameter p) {
          return visitDefault(p);
        }
      }
    }

    // These parameter descriptors are constant
    private static final BoundedWindowParameter BOUNDED_WINDOW_PARAMETER =
        new BoundedWindowParameter() {};
    private static final RestrictionTrackerParameter RESTRICTION_TRACKER_PARAMETER =
        new RestrictionTrackerParameter() {};

    public static BoundedWindowParameter boundedWindow() {
      return BOUNDED_WINDOW_PARAMETER;
    }

    public static InputProviderParameter inputProvider() {
      return new InputProviderParameter() {};
    }

    public static OutputReceiverParameter outputReceiver() {
      return new OutputReceiverParameter() {};
    }

    public static RestrictionTrackerParameter restrictionTracker() {
      return RESTRICTION_TRACKER_PARAMETER;
    }

    public static StateParameter stateParameter(StateDeclaration decl) {
      return new AutoValue_DoFnSignature_Parameter_StateParameter(decl);
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link BoundedWindow}.
     */
    public abstract static class BoundedWindowParameter extends Parameter {
      private BoundedWindowParameter() {}
    }

    /** Descriptor for a {@link Parameter} of type {@link InputProvider}. */
    public abstract static class InputProviderParameter extends Parameter {
      private InputProviderParameter() {}
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link OutputReceiver}.
     */
    public abstract static class OutputReceiverParameter extends Parameter {
      private OutputReceiverParameter() {}
    }

    /**
     * Descriptor for a {@link Parameter} of a subclass of {@link RestrictionTracker}.
     */
    public abstract static class RestrictionTrackerParameter extends Parameter {
      private RestrictionTrackerParameter() {}
    }

    /**
     * Descriptor for a {@link Parameter} of a subclass of {@link State}, with an id indicated by
     * its {@link StateId} annotation.
     */
    @AutoValue
    public abstract static class StateParameter extends Parameter {
      // Package visible for AutoValue
      StateParameter() {}
      public abstract StateDeclaration referent();
    }
  }

  /** Describes a {@link DoFn.ProcessElement} method. */
  @AutoValue
  public abstract static class ProcessElementMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Types of optional parameters of the annotated method, in the order they appear. */
    public abstract List<Parameter> extraParameters();

    /** Concrete type of the {@link RestrictionTracker} parameter, if present. */
    @Nullable
    abstract TypeToken<?> trackerT();

    /** Whether this {@link DoFn} returns a {@link ProcessContinuation} or void. */
    public abstract boolean hasReturnValue();

    static ProcessElementMethod create(
        Method targetMethod,
        List<Parameter> extraParameters,
        TypeToken<?> trackerT,
        boolean hasReturnValue) {
      return new AutoValue_DoFnSignature_ProcessElementMethod(
          targetMethod, Collections.unmodifiableList(extraParameters), trackerT, hasReturnValue);
    }

    /** Whether this {@link DoFn} uses a Single Window. */
    public boolean usesSingleWindow() {
      return extraParameters().contains(Parameter.boundedWindow());
    }

    /**
     * Whether this {@link DoFn} is <a href="https://s.apache.org/splittable-do-fn">splittable</a>.
     */
    public boolean isSplittable() {
      return extraParameters().contains(Parameter.restrictionTracker());
    }
  }

  /** Describes a {@link DoFn.StartBundle} or {@link DoFn.FinishBundle} method. */
  @AutoValue
  public abstract static class BundleMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    static BundleMethod create(Method targetMethod) {
      return new AutoValue_DoFnSignature_BundleMethod(targetMethod);
    }
  }

  /**
   * Describes a state declaration; a field of type {@link StateSpec} annotated with
   * {@link DoFn.StateId}.
   */
  @AutoValue
  public abstract static class StateDeclaration {
    public abstract String id();
    public abstract Field field();
    public abstract TypeDescriptor<? extends State> stateType();

    static StateDeclaration create(
        String id, Field field, TypeDescriptor<? extends State> stateType) {
      return new AutoValue_DoFnSignature_StateDeclaration(id, field, stateType);
    }
  }

  /** Describes a {@link DoFn.Setup} or {@link DoFn.Teardown} method. */
  @AutoValue
  public abstract static class LifecycleMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    static LifecycleMethod create(Method targetMethod) {
      return new AutoValue_DoFnSignature_LifecycleMethod(targetMethod);
    }
  }

  /** Describes a {@link DoFn.GetInitialRestriction} method. */
  @AutoValue
  public abstract static class GetInitialRestrictionMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Type of the returned restriction. */
    abstract TypeToken<?> restrictionT();

    static GetInitialRestrictionMethod create(Method targetMethod, TypeToken<?> restrictionT) {
      return new AutoValue_DoFnSignature_GetInitialRestrictionMethod(targetMethod, restrictionT);
    }
  }

  /** Describes a {@link DoFn.SplitRestriction} method. */
  @AutoValue
  public abstract static class SplitRestrictionMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Type of the restriction taken and returned. */
    abstract TypeToken<?> restrictionT();

    static SplitRestrictionMethod create(Method targetMethod, TypeToken<?> restrictionT) {
      return new AutoValue_DoFnSignature_SplitRestrictionMethod(targetMethod, restrictionT);
    }
  }

  /** Describes a {@link DoFn.NewTracker} method. */
  @AutoValue
  public abstract static class NewTrackerMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Type of the input restriction. */
    abstract TypeToken<?> restrictionT();

    /** Type of the returned {@link RestrictionTracker}. */
    abstract TypeToken<?> trackerT();

    static NewTrackerMethod create(
        Method targetMethod, TypeToken<?> restrictionT, TypeToken<?> trackerT) {
      return new AutoValue_DoFnSignature_NewTrackerMethod(targetMethod, restrictionT, trackerT);
    }
  }

  /** Describes a {@link DoFn.GetRestrictionCoder} method. */
  @AutoValue
  public abstract static class GetRestrictionCoderMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Type of the returned {@link Coder}. */
    abstract TypeToken<?> coderT();

    static GetRestrictionCoderMethod create(Method targetMethod, TypeToken<?> coderT) {
      return new AutoValue_DoFnSignature_GetRestrictionCoderMethod(targetMethod, coderT);
    }
  }
}
