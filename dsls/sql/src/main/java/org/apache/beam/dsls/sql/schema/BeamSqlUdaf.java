package org.apache.beam.dsls.sql.schema;

import java.io.Serializable;

/**
 * abstract class of aggregation functions in Beam SQL.
 *
 * <p>There're several constrains for a UDAF:<br>
 * 1. A constructor with an empty argument list is required;<br>
 * 2. The type of {@code InputT} and {@code OutputT} can only be Interger/Long/Short/Byte/Double
 * /Float/Date, mapping as SQL type INTEGER/BIGINT/SMALLINT/TINYINE/DOUBLE/FLOAT/TIMESTAMP;<br>
 * 3. wrap intermediate data in a {@link BeamSqlRow}, and do not rely on elements in class;<br>
 */
public abstract class BeamSqlUdaf<InputT, OutputT> implements Serializable {
  public BeamSqlUdaf(){}

  public abstract BeamSqlRow init();

  public abstract BeamSqlRow add(BeamSqlRow accumulator, InputT input);

  public abstract BeamSqlRow merge(Iterable<BeamSqlRow> accumulators);

  public abstract OutputT result(BeamSqlRow accumulator);
}
