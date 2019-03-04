package org.apache.beam.sdk.schemas.transforms;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;

public class Join {
  public <LhsT, RhsT> Inner innerJoin(PCollection<RhsT> rhs) {
    return new Inner<LhsT, RhsT>(JoinType.INNER, rhs);
  }

  public <LhsT, RhsT> Inner fullOuterJoin(PCollection<RhsT> rhs) {
    return new Inner<LhsT, RhsT>(JoinType.OUTER, rhs);
  }

  public <LhsT, RhsT> Inner leftOuterJoin(PCollection<RhsT> rhs) {
    return new Inner<LhsT, RhsT>(JoinType.LEFT_OUTER, rhs);
  }

  public <LhsT, RhsT> Inner rightOuterJoin(PCollection<RhsT> rhs) {
    return new Inner<LhsT, RhsT>(JoinType.RIGHT_OUTER, rhs);
  },

  private static enum JoinType {INNER, OUTER, LEFT_OUTER, RIGHT_OUTER };
  public static class Inner<LhsT, RhsT> extends PTransform<PCollection<LhsT>, PCollection<Row>> {
    private final JoinType joinType;
    private final PCollection<RhsT> rhs;

    private Inner(JoinType joinType, PCollection<RhsT> rhs) {
      this.joinType = joinType;
      this.rhs = rhs;
    }

    public Inner<LhsT, RhsT> byFieldNames(String... fieldNames) {

    }

    public Inner<LhsT, RhsT> byFieldIds(String... fieldNames) {

    }

    public Inner<LhsT, RhsT> byFieldAccessDescriptor(String... fieldNames) {

    }

    public Inner<LhsT, RhsT> byFieldNames(String... fieldNames) {

    }

    @Override
    public PCollection<Row> expand(PCollection lhs) {
      PCollectionTuple tuple = PCollectionTuple.of("lhs", lhs).and("rhs", rhs);
      return tuple.apply(CoGroup.)
      return null;
    }
  }
}
