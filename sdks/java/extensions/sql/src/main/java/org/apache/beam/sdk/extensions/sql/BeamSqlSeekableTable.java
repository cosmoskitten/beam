package org.apache.beam.sdk.extensions.sql;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.values.BeamRecord;

/**
 * A seekable table converts a JOIN operator to an inline lookup.
 * It's triggered by {@code SELECT * FROM FACT_TABLE JOIN LOOKUP_TABLE ON ...}.
 */
@Experimental
public interface BeamSqlSeekableTable extends Serializable{
  /**
   * return a list of {@code BeamRecord} with given key set.
   */
  List<BeamRecord> seekRecord(BeamRecord lookupSubRecord);
}
