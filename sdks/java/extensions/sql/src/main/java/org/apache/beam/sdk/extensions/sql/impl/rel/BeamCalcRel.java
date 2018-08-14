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
package org.apache.beam.sdk.extensions.sql.impl.rel;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.schemas.Schema.FieldType;
import static org.apache.beam.sdk.schemas.Schema.TypeName;
import static org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelDataTypeSystem;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.GotoExpressionKind;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ScriptEvaluator;
import org.joda.time.DateTime;
import org.joda.time.base.AbstractDateTime;

/** BeamRelNode to replace a {@code Project} node. */
public class BeamCalcRel extends Calc implements BeamRelNode {

  private static final ParameterExpression outputSchemaParam =
      Expressions.parameter(Schema.class, "outputSchema");
  private static final ParameterExpression processContextParam =
      Expressions.parameter(DoFn.ProcessContext.class, "c");

  public BeamCalcRel(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexProgram program) {
    super(cluster, traits, input, program);
  }

  @Override
  public Calc copy(RelTraitSet traitSet, RelNode input, RexProgram program) {
    return new BeamCalcRel(getCluster(), traitSet, input, program);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new Transform();
  }

  private static final Map<TypeName, Type> rawTypeMap =
      ImmutableMap.<TypeName, Type>builder()
          .put(TypeName.BYTE, Byte.class)
          .put(TypeName.INT16, Short.class)
          .put(TypeName.INT32, Integer.class)
          .put(TypeName.INT64, Long.class)
          .put(TypeName.FLOAT, Float.class)
          .put(TypeName.DOUBLE, Double.class)
          .build();

  private class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    /** expand is based on calcite's EnumerableCalc.implement() */
    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      checkArgument(
          pinput.size() == 1,
          "Wrong number of inputs for %s: %s",
          BeamCalcRel.class.getSimpleName(),
          pinput);
      PCollection<Row> upstream = pinput.get(0);
      Schema outputSchema = CalciteUtils.toSchema(getRowType());

      final JavaTypeFactory typeFactory =
          new JavaTypeFactoryImpl(BeamRelDataTypeSystem.INSTANCE) {
            @Override
            public Type getJavaClass(RelDataType type) {
              if (type instanceof BasicSqlType || type instanceof IntervalSqlType) {
                if (type.getSqlTypeName() == SqlTypeName.FLOAT) {
                  return type.isNullable() ? Float.class : float.class;
                }
              }
              return super.getJavaClass(type);
            }
          };
      final BlockBuilder builder = new BlockBuilder();

      final PhysType physType =
          PhysTypeImpl.of(typeFactory, getRowType(), JavaRowFormat.ARRAY, false);

      Expression input =
          Expressions.convert_(Expressions.call(processContextParam, "element"), Row.class);

      final RexBuilder rexBuilder = getCluster().getRexBuilder();
      final RelMetadataQuery mq = RelMetadataQuery.instance();
      final RelOptPredicateList predicates = mq.getPulledUpPredicates(getInput());
      final RexSimplify simplify = new RexSimplify(rexBuilder, predicates, false, RexUtil.EXECUTOR);
      final RexProgram program = BeamCalcRel.this.program.normalize(rexBuilder, simplify);

      Expression condition =
          RexToLixTranslator.translateCondition(
              program,
              typeFactory,
              builder,
              new InputGetterImpl(input, upstream.getSchema()),
              null);

      List<Expression> expressions =
          RexToLixTranslator.translateProjects(
              program,
              typeFactory,
              builder,
              physType,
              DataContext.ROOT,
              new InputGetterImpl(input, upstream.getSchema()),
              null);
      assert expressions.size() == outputSchema.getFieldCount();

      // output = Row.withSchema(outputSchema)
      Expression output = Expressions.call(Row.class, "withSchema", outputSchemaParam);
      Method addValue = Types.lookupMethod(Row.Builder.class, "addValue", Object.class);

      for (int index = 0; index < expressions.size(); index++) {
        Expression value = expressions.get(index);
        FieldType toType = outputSchema.getField(index).getType();

        if (value.getType() == Object.class) {
          // just pass object through
        } else if (toType.getTypeName().isDateType()
            && value.getType() instanceof Class
            && !Types.isAssignableFrom(AbstractDateTime.class, (Class) value.getType())) {
          Expression valueDateTime;
          if (toType.getMetadata() == null
              || new String(toType.getMetadata(), UTF_8).equals("TIME")) {
            valueDateTime = value;
          } else if (new String(toType.getMetadata(), UTF_8).equals("DATE")) {
            valueDateTime = Expressions.multiply(value, Expressions.constant(MILLIS_PER_DAY));
          } else {
            throw new IllegalArgumentException(
                "Unknown DateTime type " + new String(toType.getMetadata(), UTF_8));
          }
          valueDateTime = Expressions.new_(DateTime.class, valueDateTime);

          if (((Class) value.getType()).isPrimitive()) {
            value = valueDateTime;
          } else {
            value =
                Expressions.condition(
                    Expressions.equal(value, Expressions.constant(null)),
                    Expressions.constant(null),
                    valueDateTime);
          }
        } else if (toType.getTypeName() == TypeName.DECIMAL
            && value.getType() instanceof Class
            && !Types.isAssignableFrom(BigDecimal.class, (Class) value.getType())) {
          value = Expressions.new_(BigDecimal.class, value);

        } else if (value.getType() instanceof Class
            && (((Class) value.getType()).isPrimitive()
                || Types.isAssignableFrom(Number.class, (Class) value.getType()))) {
          Type rawType = rawTypeMap.get(toType.getTypeName());
          if (rawType != null) {
            value = Types.castIfNecessary(rawType, value);
          }
        }

        // .addValue(value)
        output = Expressions.call(output, addValue, value);
      }

      // .build();
      output = Expressions.call(output, "build");

      // if (condition) {
      //   c.output(output);
      // }
      builder.add(
          Expressions.ifThen(
              condition,
              Expressions.makeGoto(
                  GotoExpressionKind.Sequence,
                  null,
                  Expressions.call(
                      processContextParam,
                      Types.lookupMethod(DoFn.ProcessContext.class, "output", Object.class),
                      output))));

      CalcFn calcFn = new CalcFn(builder.toBlock().toString(), outputSchema);

      // validate generated code
      calcFn.compile();

      PCollection<Row> projectStream = upstream.apply(ParDo.of(calcFn)).setRowSchema(outputSchema);

      return projectStream;
    }
  }

  public int getLimitCountOfSortRel() {
    if (input instanceof BeamSortRel) {
      return ((BeamSortRel) input).getCount();
    }

    throw new RuntimeException("Could not get the limit count from a non BeamSortRel input.");
  }

  public boolean isInputSortRelAndLimitOnly() {
    return (input instanceof BeamSortRel) && ((BeamSortRel) input).isLimitOnly();
  }

  /** {@code CalcFn} is the executor for a {@link BeamCalcRel} step. */
  private static class CalcFn extends DoFn<Row, Row> {
    private final String processElementBlock;
    private final Schema outputSchema;
    private transient @Nullable ScriptEvaluator se = null;

    public CalcFn(String processElementBlock, Schema outputSchema) {
      this.processElementBlock = processElementBlock;
      this.outputSchema = outputSchema;
    }

    ScriptEvaluator compile() {
      ScriptEvaluator se = new ScriptEvaluator();
      se.setParameters(
          new String[] {outputSchemaParam.name, processContextParam.name, DataContext.ROOT.name},
          new Class[] {
            (Class) outputSchemaParam.getType(),
            (Class) processContextParam.getType(),
            (Class) DataContext.ROOT.getType()
          });
      try {
        se.cook(processElementBlock);
      } catch (CompileException e) {
        throw new RuntimeException("Could not compile CalcFn: " + processElementBlock, e);
      }
      return se;
    }

    @Setup
    public void setup() {
      this.se = compile();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      assert se != null;
      try {
        se.evaluate(new Object[] {outputSchema, c, CONTEXT_INSTANCE});
      } catch (InvocationTargetException e) {
        throw new RuntimeException(
            "CalcFn failed to evaluate: " + processElementBlock, e.getCause());
      }
    }
  }

  private static class InputGetterImpl implements RexToLixTranslator.InputGetter {
    private static final Map<TypeName, String> typeGetterMap =
        ImmutableMap.<TypeName, String>builder()
            .put(TypeName.BYTE, "getByte")
            .put(TypeName.BYTES, "getBytes")
            .put(TypeName.INT16, "getInt16")
            .put(TypeName.INT32, "getInt32")
            .put(TypeName.INT64, "getInt64")
            .put(TypeName.DECIMAL, "getDecimal")
            .put(TypeName.FLOAT, "getFloat")
            .put(TypeName.DOUBLE, "getDouble")
            .put(TypeName.STRING, "getString")
            .put(TypeName.DATETIME, "getDateTime")
            .put(TypeName.BOOLEAN, "getBoolean")
            .put(TypeName.MAP, "getMap")
            .put(TypeName.ARRAY, "getArray")
            .put(TypeName.ROW, "getRow")
            .build();

    private final Expression input;
    private final Schema inputSchema;

    private InputGetterImpl(Expression input, Schema inputSchema) {
      this.input = input;
      this.inputSchema = inputSchema;
    }

    @Override
    public Expression field(BlockBuilder list, int index, Type storageType) {
      if (index >= inputSchema.getFieldCount() || index < 0) {
        throw new IllegalArgumentException("Unable to find field #" + index);
      }

      final Expression expression = list.append("current", input);
      if (storageType == Object.class) {
        return Expressions.convert_(
            Expressions.call(expression, "getValue", Expressions.constant(index)), Object.class);
      }
      FieldType fromType = inputSchema.getField(index).getType();
      String getter = typeGetterMap.get(fromType.getTypeName());
      if (getter == null) {
        throw new IllegalArgumentException("Unable to get " + fromType.getTypeName());
      }

      Expression field = Expressions.call(expression, getter, Expressions.constant(index));
      if (fromType.getTypeName().isDateType()) {
        if (fromType.getMetadata() == null
            || new String(fromType.getMetadata(), UTF_8).equals("TIME")) {
          field = Expressions.call(field, "getMillis");
        } else if (new String(fromType.getMetadata(), UTF_8).equals("DATE")) {
          field = Expressions.call(field, "getMillis");
          field = Expressions.modulo(field, Expressions.constant(MILLIS_PER_DAY));
        } else {
          throw new IllegalArgumentException(
              "Unknown DateTime type " + new String(fromType.getMetadata(), UTF_8));
        }
      }
      return field;
    }
  }

  private static final DataContext CONTEXT_INSTANCE = new SlimDataContext();

  private static class SlimDataContext implements DataContext {
    @Override
    public SchemaPlus getRootSchema() {
      return null;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
      return null;
    }

    @Override
    public QueryProvider getQueryProvider() {
      return null;
    }

    @Override
    public Object get(String name) {
      if (name.equals(DataContext.Variable.UTC_TIMESTAMP.camelName)
          || name.equals(DataContext.Variable.CURRENT_TIMESTAMP.camelName)
          || name.equals(DataContext.Variable.LOCAL_TIMESTAMP.camelName)) {
        return System.currentTimeMillis();
      }
      return null;
    }
  }
}
