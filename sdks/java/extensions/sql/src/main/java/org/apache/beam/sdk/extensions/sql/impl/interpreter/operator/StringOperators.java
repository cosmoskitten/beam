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

package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator;

import java.util.List;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;

/** String operator implementations */
public class StringOperators {

  public interface StringOperator extends BeamSqlOperator {
    default SqlTypeName getOutputType() {
      return SqlTypeName.VARCHAR;
    }
  }

  @FunctionalInterface
  private interface StringUnaryOperator extends BeamSqlUnaryOperator {
    default boolean accept(BeamSqlExpression arg) {
      return SqlTypeName.CHAR_TYPES.contains(arg.getOutputType());
    }

    default SqlTypeName getOutputType() {
      return SqlTypeName.VARCHAR;
    }
  }

  public static final BeamSqlOperator CHAR_LENGTH =
      (StringUnaryOperator)
          (BeamSqlPrimitive arg) ->
              BeamSqlPrimitive.of(SqlTypeName.INTEGER, arg.getString().length());

  public static final BeamSqlOperator UPPER =
      (StringUnaryOperator)
          (BeamSqlPrimitive arg) ->
              BeamSqlPrimitive.of(SqlTypeName.VARCHAR, arg.getString().toUpperCase());

  public static final BeamSqlOperator LOWER =
      (StringUnaryOperator)
          (BeamSqlPrimitive arg) ->
              BeamSqlPrimitive.of(SqlTypeName.VARCHAR, arg.getString().toLowerCase());

  /** {@code INITCAP}. */
  public static final BeamSqlOperator INIT_CAP =
      (StringUnaryOperator)
          (BeamSqlPrimitive arg) -> {
            String str = arg.getString();

            StringBuilder ret = new StringBuilder(str);
            boolean isInit = true;
            for (int i = 0; i < str.length(); i++) {
              if (Character.isWhitespace(str.charAt(i))) {
                isInit = true;
                continue;
              }

              if (isInit) {
                ret.setCharAt(i, Character.toUpperCase(str.charAt(i)));
                isInit = false;
              } else {
                ret.setCharAt(i, Character.toLowerCase(str.charAt(i)));
              }
            }
            return BeamSqlPrimitive.of(SqlTypeName.VARCHAR, ret.toString());
          };

  public static final BeamSqlBinaryOperator CONCAT =
      new BeamSqlBinaryOperator() {
        @Override
        public SqlTypeName getOutputType() {
          return SqlTypeName.VARCHAR;
        }

        @Override
        public boolean accept(BeamSqlExpression left, BeamSqlExpression right) {
          return SqlTypeName.CHAR_TYPES.contains(left.getOutputType())
              && SqlTypeName.CHAR_TYPES.contains(right.getOutputType());
        }

        @Override
        public BeamSqlPrimitive apply(BeamSqlPrimitive left, BeamSqlPrimitive right) {
          String leftString = left.getString();
          String rightString = right.getString();

          return BeamSqlPrimitive.of(
              SqlTypeName.VARCHAR,
              new StringBuilder(leftString.length() + rightString.length())
                  .append(leftString)
                  .append(rightString)
                  .toString());
        }
      };

  public static final BeamSqlOperator POSITION =
      new StringOperator() {
        @Override
        public boolean accept(List<BeamSqlExpression> operands) {
          if (operands.size() < 2 || operands.size() > 3) {
            return false;
          }

          return SqlTypeName.CHAR_TYPES.contains(operands.get(0).getOutputType())
              && SqlTypeName.CHAR_TYPES.contains(operands.get(1).getOutputType())
              && ((operands.size() < 3)
                  || SqlTypeName.INT_TYPES.contains(operands.get(2).getOutputType()));
        }

        @Override
        public BeamSqlPrimitive apply(List<BeamSqlPrimitive> arguments) {
          String targetStr = arguments.get(0).getString();
          String containingStr = arguments.get(1).getString();
          int from = arguments.size() < 3 ? -1 : ((Number) arguments.get(2).getValue()).intValue();

          int idx = containingStr.indexOf(targetStr, from);

          return BeamSqlPrimitive.of(SqlTypeName.INTEGER, idx);
        }
      };

  public static final BeamSqlOperator TRIM =
      new StringOperator() {
        @Override
        public boolean accept(List<BeamSqlExpression> subexpressions) {
          return (subexpressions.size() == 1
                  && SqlTypeName.CHAR_TYPES.contains(subexpressions.get(0).getOutputType()))
              || (subexpressions.size() == 3
                  && SqlTypeName.SYMBOL.equals(subexpressions.get(0).getOutputType())
                  && SqlTypeName.CHAR_TYPES.contains(subexpressions.get(1).getOutputType())
                  && SqlTypeName.CHAR_TYPES.contains(subexpressions.get(2).getOutputType()));
        }

        @Override
        public BeamSqlPrimitive apply(List<BeamSqlPrimitive> operands) {
          if (operands.size() == 1) {
            return BeamSqlPrimitive.of(
                SqlTypeName.VARCHAR, operands.get(0).getValue().toString().trim());
          } else {
            SqlTrimFunction.Flag type = (SqlTrimFunction.Flag) operands.get(0).getValue();
            String targetStr = operands.get(1).getString();
            String containingStr = operands.get(2).getString();

            switch (type) {
              case LEADING:
                return BeamSqlPrimitive.of(
                    SqlTypeName.VARCHAR, leadingTrim(containingStr, targetStr));
              case TRAILING:
                return BeamSqlPrimitive.of(
                    SqlTypeName.VARCHAR, trailingTrim(containingStr, targetStr));
              case BOTH:
              default:
                return BeamSqlPrimitive.of(
                    SqlTypeName.VARCHAR,
                    trailingTrim(leadingTrim(containingStr, targetStr), targetStr));
            }
          }
        }

        String leadingTrim(String containingStr, String targetStr) {
          int idx = 0;
          while (containingStr.startsWith(targetStr, idx)) {
            idx += targetStr.length();
          }

          return containingStr.substring(idx);
        }

        String trailingTrim(String containingStr, String targetStr) {
          int idx = containingStr.length() - targetStr.length();
          while (containingStr.startsWith(targetStr, idx)) {
            idx -= targetStr.length();
          }

          idx += targetStr.length();
          return containingStr.substring(0, idx);
        }
      };

  public static final BeamSqlOperator OVERLAY =
      new StringOperator() {
        @Override
        public boolean accept(List<BeamSqlExpression> operands) {
          return (operands.size() == 3
                  && SqlTypeName.CHAR_TYPES.contains(operands.get(0).getOutputType())
                  && SqlTypeName.CHAR_TYPES.contains(operands.get(1).getOutputType())
                  && SqlTypeName.INT_TYPES.contains(operands.get(2).getOutputType()))
              || (operands.size() == 4
                      && SqlTypeName.CHAR_TYPES.contains(operands.get(0).getOutputType())
                      && SqlTypeName.CHAR_TYPES.contains(operands.get(1).getOutputType())
                      && SqlTypeName.INT_TYPES.contains(operands.get(2).getOutputType()))
                  && SqlTypeName.INT_TYPES.contains(operands.get(3).getOutputType());
        }

        @Override
        public BeamSqlPrimitive apply(List<BeamSqlPrimitive> operands) {
          String str = operands.get(0).getString();
          String replaceStr = operands.get(1).getString();
          int idx = operands.get(2).getInteger();

          // the index is 1 based.
          idx -= 1;
          int length = replaceStr.length();
          if (operands.size() == 4) {
            length = operands.get(3).getInteger();
          }

          StringBuilder result = new StringBuilder(str.length() + replaceStr.length() - length);
          result
              .append(str.substring(0, idx))
              .append(replaceStr)
              .append(str.substring(idx + length));

          return BeamSqlPrimitive.of(SqlTypeName.VARCHAR, result.toString());
        }
      };

  public static final BeamSqlOperator SUBSTRING =
      new StringOperator() {

        @Override
        public boolean accept(List<BeamSqlExpression> operands) {
          return (operands.size() == 2
                  && SqlTypeName.CHAR_TYPES.contains(operands.get(0).getOutputType())
                  && SqlTypeName.INT_TYPES.contains(operands.get(1).getOutputType()))
              || (operands.size() == 3
                  && SqlTypeName.CHAR_TYPES.contains(operands.get(0).getOutputType())
                  && SqlTypeName.INT_TYPES.contains(operands.get(1).getOutputType())
                  && SqlTypeName.INT_TYPES.contains(operands.get(2).getOutputType()));
        }

        @Override
        public BeamSqlPrimitive apply(List<BeamSqlPrimitive> operands) {
          String str = operands.get(0).getString();
          int idx = operands.get(1).getInteger();
          int startIdx = idx;
          if (startIdx > 0) {
            // NOTE: SQL substring is 1 based(rather than 0 based)
            startIdx -= 1;
          } else if (startIdx < 0) {
            // NOTE: SQL also support negative index...
            startIdx += str.length();
          } else {
            return BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "");
          }

          if (operands.size() == 3) {
            int length = operands.get(2).getInteger();
            if (length < 0) {
              length = 0;
            }
            int endIdx = Math.min(startIdx + length, str.length());
            return BeamSqlPrimitive.of(SqlTypeName.VARCHAR, str.substring(startIdx, endIdx));
          } else {
            return BeamSqlPrimitive.of(SqlTypeName.VARCHAR, str.substring(startIdx));
          }
        }
      };
}
