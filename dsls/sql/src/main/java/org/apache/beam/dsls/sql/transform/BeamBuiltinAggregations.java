package org.apache.beam.dsls.sql.transform;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.dsls.sql.schema.BeamSqlUdaf;
import org.apache.beam.dsls.sql.utils.CalciteUtils;

/**
 * Build-in aggregations functions for COUNT/MAX/MIN/SUM/AVG.
 */
class BeamBuiltinAggregations {
  /**
   * Build-in aggregation for COUNT.
   */
  public static class Count<T> extends BeamSqlUdaf<T, Long> {
    private BeamSqlRecordType accType;

    public Count() {
      accType = BeamSqlRecordType.create(Arrays.asList("__count"), Arrays.asList(Types.BIGINT));
    }

    @Override
    public BeamSqlRow init() {
      return new BeamSqlRow(accType, Arrays.<Object>asList(0L));
    }

    @Override
    public BeamSqlRow add(BeamSqlRow accumulator, T input) {
      return new BeamSqlRow(accType, Arrays.<Object>asList(accumulator.getLong(0) + 1));
    }

    @Override
    public BeamSqlRow merge(Iterable<BeamSqlRow> accumulators) {
      long v = 0L;
      while (accumulators.iterator().hasNext()) {
        v += accumulators.iterator().next().getLong(0);
      }
      return new BeamSqlRow(accType, Arrays.<Object>asList(v));
    }

    @Override
    public Long result(BeamSqlRow accumulator) {
      return accumulator.getLong(0);
    }
  }

  /**
   * Build-in aggregation for MAX.
   */
  public static class Max<T extends Comparable<T>> extends BeamSqlUdaf<T, T> {
    private BeamSqlRecordType accType;

    public Max(int outputFieldType) {
      this.accType = BeamSqlRecordType.create(Arrays.asList("__max"),
          Arrays.asList(outputFieldType));
    }

    @Override
    public BeamSqlRow init() {
      return null;
    }

    @Override
    public BeamSqlRow add(BeamSqlRow accumulator, T input) {
      return new BeamSqlRow(accType,
          Arrays
              .<Object>asList((accumulator == null || ((Comparable<T>) accumulator.getFieldValue(0))
                  .compareTo(input) < 0)
                      ? input : accumulator.getFieldValue(0)));
    }

    @Override
    public BeamSqlRow merge(Iterable<BeamSqlRow> accumulators) {
      T mergedV = (T) accumulators.iterator().next().getFieldValue(0);
      while (accumulators.iterator().hasNext()) {
        T v = (T) accumulators.iterator().next().getFieldValue(0);
        mergedV = mergedV.compareTo(v) > 0 ? mergedV : v;
      }
      return new BeamSqlRow(accType, Arrays.<Object>asList(mergedV));
    }

    @Override
    public T result(BeamSqlRow accumulator) {
      return (T) accumulator.getFieldValue(0);
    }
  }

  /**
   * Build-in aggregation for MIN.
   */
  public static class Min<T extends Comparable<T>> extends BeamSqlUdaf<T, T> {
    private BeamSqlRecordType accType;

    public Min(int outputFieldType) {
      this.accType = BeamSqlRecordType.create(Arrays.asList("__min"),
          Arrays.asList(outputFieldType));
    }

    @Override
    public BeamSqlRow init() {
      return null;
    }

    @Override
    public BeamSqlRow add(BeamSqlRow accumulator, T input) {
      return new BeamSqlRow(accType,
          Arrays
              .<Object>asList((accumulator == null || ((Comparable<T>) accumulator.getFieldValue(0))
                  .compareTo(input) > 0)
                      ? input : accumulator.getFieldValue(0)));
    }

    @Override
    public BeamSqlRow merge(Iterable<BeamSqlRow> accumulators) {
      T mergedV = (T) accumulators.iterator().next().getFieldValue(0);
      while (accumulators.iterator().hasNext()) {
        T v = (T) accumulators.iterator().next().getFieldValue(0);
        mergedV = mergedV.compareTo(v) < 0 ? mergedV : v;
      }
      return new BeamSqlRow(accType, Arrays.<Object>asList(mergedV));
    }

    @Override
    public T result(BeamSqlRow accumulator) {
      return (T) accumulator.getFieldValue(0);
    }
  }

  /**
   * Build-in aggregation for SUM.
   */
  public static class Sum<T> extends BeamSqlUdaf<T, T> {
    private static List<Integer> supportedType = Arrays.asList(Types.INTEGER,
        Types.BIGINT, Types.SMALLINT, Types.TINYINT, Types.DOUBLE,
        Types.FLOAT);

    private int outputFieldType;
    private BeamSqlRecordType accType;
    public Sum(int outputFieldType) {
      //check input data type is supported
      if (!supportedType.contains(outputFieldType)) {
        throw new UnsupportedOperationException(String.format(
            "data type [%s] is not supported in SUM", CalciteUtils.toCalciteType(outputFieldType)));
      }

      this.outputFieldType = outputFieldType;
      this.accType = BeamSqlRecordType.create(Arrays.asList("__sum"),
          Arrays.asList(Types.DOUBLE)); //by default use DOUBLE to store the value.
    }

    @Override
    public BeamSqlRow init() {
      return new BeamSqlRow(accType, Arrays.<Object>asList(0.0));
    }

    @Override
    public BeamSqlRow add(BeamSqlRow accumulator, T input) {
      return new BeamSqlRow(accType, Arrays.<Object>asList(accumulator.getDouble(0)
          + Double.valueOf(input.toString())));
    }

    @Override
    public BeamSqlRow merge(Iterable<BeamSqlRow> accumulators) {
      double v = 0.0;
      while (accumulators.iterator().hasNext()) {
        v += accumulators.iterator().next().getDouble(0);
      }
      return new BeamSqlRow(accType, Arrays.<Object>asList(v));
    }

    @Override
    public T result(BeamSqlRow accumulator) {
      BeamSqlRow result = new BeamSqlRow(
          BeamSqlRecordType.create(Arrays.asList("__sum"), Arrays.asList(outputFieldType)));
      switch (outputFieldType) {
      case Types.INTEGER:
        result.addField(0, (int) accumulator.getDouble(0));
        break;
      case Types.BIGINT:
        result.addField(0, (long) accumulator.getDouble(0));
        break;
      case Types.SMALLINT:
        result.addField(0, (short) accumulator.getDouble(0));
        break;
      case Types.TINYINT:
        result.addField(0, (byte) accumulator.getDouble(0));
        break;
      case Types.DOUBLE:
        result.addField(0, accumulator.getDouble(0));
        break;
      case Types.FLOAT:
        result.addField(0, (float) accumulator.getDouble(0));
        break;

      default:
        break;
      }
      return (T) result.getFieldValue(0);
    }

  }

  /**
   * Build-in aggregation for AVG.
   */
  public static class Avg<T> extends BeamSqlUdaf<T, T> {
    private static List<Integer> supportedType = Arrays.asList(Types.INTEGER,
        Types.BIGINT, Types.SMALLINT, Types.TINYINT, Types.DOUBLE,
        Types.FLOAT);

    private int outputFieldType;
    private BeamSqlRecordType accType;
    public Avg(int outputFieldType) {
      //check input data type is supported
      if (!supportedType.contains(outputFieldType)) {
        throw new UnsupportedOperationException(String.format(
            "data type [%s] is not supported in AVG", CalciteUtils.toCalciteType(outputFieldType)));
      }

      this.outputFieldType = outputFieldType;
      this.accType = BeamSqlRecordType.create(Arrays.asList("__sum", "size"),
          Arrays.asList(Types.DOUBLE, Types.BIGINT)); //by default use DOUBLE to store the value.
    }

    @Override
    public BeamSqlRow init() {
      return new BeamSqlRow(accType, Arrays.<Object>asList(0.0, 0L));
    }

    @Override
    public BeamSqlRow add(BeamSqlRow accumulator, T input) {
      return new BeamSqlRow(accType,
          Arrays.<Object>asList(
              accumulator.getDouble(0)
                  + Double.valueOf(input.toString()),
              accumulator.getLong(1) + 1));
    }

    @Override
    public BeamSqlRow merge(Iterable<BeamSqlRow> accumulators) {
      double v = 0.0;
      long s = 0;
      while (accumulators.iterator().hasNext()) {
        BeamSqlRow r = accumulators.iterator().next();
        v += r.getDouble(0);
        s += r.getLong(1);
      }
      return new BeamSqlRow(accType, Arrays.<Object>asList(v, s));
    }

    @Override
    public T result(BeamSqlRow accumulator) {
      BeamSqlRow result = new BeamSqlRow(
          BeamSqlRecordType.create(Arrays.asList("__avg"), Arrays.asList(outputFieldType)));
      switch (outputFieldType) {
      case Types.INTEGER:
        result.addField(0, (int) (accumulator.getDouble(0) / accumulator.getLong(1)));
        break;
      case Types.BIGINT:
        result.addField(0, (long) (accumulator.getDouble(0) / accumulator.getLong(1)));
        break;
      case Types.SMALLINT:
        result.addField(0, (short) (accumulator.getDouble(0) / accumulator.getLong(1)));
        break;
      case Types.TINYINT:
        result.addField(0, (byte) (accumulator.getDouble(0) / accumulator.getLong(1)));
        break;
      case Types.DOUBLE:
        result.addField(0, accumulator.getDouble(0) / accumulator.getLong(1));
        break;
      case Types.FLOAT:
        result.addField(0, (float) (accumulator.getDouble(0) / accumulator.getLong(1)));
        break;

      default:
        break;
      }
      return (T) result.getFieldValue(0);
    }

  }

}
