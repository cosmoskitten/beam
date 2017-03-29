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

package org.apache.beam.runners.core.construction;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Utilities for interacting with {@link ParDos}.
 */
public class ParDos {
  public static <InputT> PCollection<InputT> getMainInput(
      Map<TupleTag<?>, PValue> inputs, ParDo.SingleOutput<InputT, ?> parDo) {
    return getMainInput(inputs, parDo.getSideInputs());
  }

  public static <InputT> PCollection<InputT> getMainInput(
      Map<TupleTag<?>, PValue> inputs, ParDo.MultiOutput<InputT, ?> parDo) {
    return getMainInput(inputs, parDo.getSideInputs());
  }

  private static <InputT> PCollection<InputT> getMainInput(
      Map<TupleTag<?>, PValue> inputs, List<PCollectionView<?>> sideInputs) {
    Set<TupleTag<?>> sideInputTags = new HashSet<>();
    for (PCollectionView<?> view : sideInputs) {
      sideInputTags.add(view.getTagInternal());
    }
    PCollection<InputT> mainInput = null;
    for (Map.Entry<TupleTag<?>, PValue> input : inputs.entrySet()) {
      if (!sideInputTags.contains(input.getKey())) {
        checkArgument(
            mainInput == null,
            "Got multiple inputs that are not side inputs for a %s Main Input: %s and %s",
            ParDo.class.getSimpleName(),
            mainInput,
            input.getValue());
        checkArgument(
            input.getValue() instanceof PCollection,
            "Unexpected input type %s",
            input.getValue().getClass().getSimpleName());
        mainInput = (PCollection<InputT>) input.getValue();
      }
    }
    checkArgument(
        mainInput != null,
        "No main input found in inputs: Inputs %s, Side Input tags %s",
        inputs,
        sideInputTags);
    return mainInput;
  }

}
