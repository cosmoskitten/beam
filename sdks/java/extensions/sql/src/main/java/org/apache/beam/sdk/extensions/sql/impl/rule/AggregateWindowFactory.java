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

package org.apache.beam.sdk.extensions.sql.impl.rule;

import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.joda.time.Duration;

/**
 * Creates Beam WindowFn based on HOP/TUMBLE/SESSION call in a query.
 */
class AggregateWindowFactory {

  static AggregateWindowField getWindowFieldAt(RexCall call, int groupField) {

    Optional<WindowFn> window = createWindowFn(call.operands, call.op.kind);

    if (!window.isPresent()) {
      return AggregateWindowField.absent();
    }

    return
        AggregateWindowField
            .builder()
            .setFieldIndex(groupField)
            .setWindowFn(window.get())
            .build();
  }

  private static Optional<WindowFn> createWindowFn(List<RexNode> parameters, SqlKind operatorKind) {
    switch (operatorKind) {
      case TUMBLE:
        FixedWindows fixedWindows = FixedWindows.of(durationParameter(parameters, 1));
        if (parameters.size() == 3) {
          fixedWindows = fixedWindows.withOffset(durationParameter(parameters, 2));
        }

        return Optional.of(fixedWindows);
      case HOP:
        SlidingWindows slidingWindows = SlidingWindows
            .of(durationParameter(parameters, 1))
            .every(durationParameter(parameters, 2));

        if (parameters.size() == 4) {
          slidingWindows = slidingWindows.withOffset(durationParameter(parameters, 4));
        }

        return Optional.of(slidingWindows);
      case SESSION:
        Sessions sessions = Sessions.withGapDuration(durationParameter(parameters, 1));
        if (parameters.size() == 3) {
          throw new UnsupportedOperationException(
              "Specifying alignment (offset) is not supported for session windows");
        }

        return Optional.of(sessions);
      default:
        return Optional.empty();
    }
  }

  private static Duration durationParameter(List<RexNode> parameters, int parameterIndex) {
    return Duration.millis(intValue(parameters.get(parameterIndex)));
  }

  private static long intValue(RexNode operand) {
    if (operand instanceof RexLiteral) {
      return RexLiteral.intValue(operand);
    } else {
      throw new IllegalArgumentException(String.format("[%s] is not valid.", operand));
    }
  }
}
