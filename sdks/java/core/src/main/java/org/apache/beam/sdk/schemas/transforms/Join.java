package org.apache.beam.sdk.schemas.transforms;

import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import java.lang.String;
import java.lang.Integer;

public class Join {
  public static class Equals {
    public static Inner lhsFieldNames(String... fieldNames) {
      return new Inner(FieldAccessDescriptor.withFieldNames(fieldNames), FieldAccessDescriptor.create());
    }

    public static Inner lhsFieldIds(Integer... fieldIds) {
      return new Inner(FieldAccessDescriptor.withFieldIds(fieldIds), FieldAccessDescriptor.create());
    }

    public static Inner lhsFieldAccessDescriptor(FieldAccessDescriptor fieldAccessDescriptor) {
      return new Inner(fieldAccessDescriptor, FieldAccessDescriptor.create());
    }

    public Inner rhsFieldNames(String... fieldNames) {
      return new Inner(FieldAccessDescriptor.create(), FieldAccessDescriptor.withFieldNames(fieldNames));
    }

    public Inner rhsFieldIds(Integer... fieldIds) {
      return new Inner(FieldAccessDescriptor.create(), FieldAccessDescriptor.withFieldIds(fieldIds));
    }

    public Inner rhsFieldAccessDescriptor(FieldAccessDescriptor fieldAccessDescriptor) {
      return new Inner(FieldAccessDescriptor.create(), fieldAccessDescriptor);
    }

    public static class Inner {
      private FieldAccessDescriptor lhs;
      private FieldAccessDescriptor rhs;

      private Inner(FieldAccessDescriptor lhs, FieldAccessDescriptor rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
      }

      public Inner lhsFieldNames(String... fieldNames) {
        return new Inner(FieldAccessDescriptor.withFieldNames(fieldNames), rhs);
      }

      public Inner lhsFieldIds(Integer... fieldIds) {
        return new Inner(FieldAccessDescriptor.withFieldIds(fieldIds), rhs);
      }

      public Inner lhsFieldAccessDescriptor(FieldAccessDescriptor fieldAccessDescriptor) {
        return new Inner(fieldAccessDescriptor, rhs);
      }

      public Inner rhsFieldNames(String... fieldNames) {
        return new Inner(lhs, FieldAccessDescriptor.withFieldNames(fieldNames));
      }

      public Inner rhsFieldIds(Integer... fieldIds) {
        return new Inner(lhs, FieldAccessDescriptor.withFieldIds(fieldIds));
      }

      public Inner rhsFieldAccessDescriptor(FieldAccessDescriptor fieldAccessDescriptor) {
        return new Inner(lhs, fieldAccessDescriptor);
      }
    }
  }

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
  };

  private static enum JoinType {INNER, OUTER, LEFT_OUTER, RIGHT_OUTER };
  public static class Inner<LhsT, RhsT> extends PTransform<PCollection<LhsT>, PCollection<Row>> {
    private final JoinType joinType;
    private final PCollection<RhsT> rhs;

    private Inner(JoinType joinType, PCollection<RhsT> rhs) {
      this.joinType = joinType;
      this.rhs = rhs;
    }
/*
    public Inner<LhsT, RhsT> byFieldNames(String... fieldNames) {

    }

    public Inner<LhsT, RhsT> byFieldIds(Integer... fieldIds) {

    }

    public Inner<LhsT, RhsT> byFieldAccessDescriptor(String... fieldNames) {

    }*/

    @Override
    public PCollection<Row> expand(PCollection lhs) {
    //  PCollectionTuple tuple = PCollectionTuple.of("lhs", lhs).and("rhs", rhs);
//      return tuple.apply(CoGroup.)
      return null;
    }
  }
}
