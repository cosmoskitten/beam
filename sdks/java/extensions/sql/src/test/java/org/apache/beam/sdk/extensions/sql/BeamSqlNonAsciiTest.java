package org.apache.beam.sdk.extensions.sql;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Test;

/**
 * Tests for non ascii char in sql.
 */
public class BeamSqlNonAsciiTest extends BeamSqlDslBase{

    @Test
    public void testNonAsciiLiteral() {
        String sql = "SELECT * FROM TABLE_A WHERE f_string = '第四行'";

        PCollection<BeamRecord> result =
                PCollectionTuple.of(new TupleTag<BeamRecord>("TABLE_A"), boundedInput1)
                        .apply("testCompositeFilter", BeamSql.queryMulti(sql));

        PAssert.that(result).containsInAnyOrder(recordsInTableA.get(3));

        pipeline.run().waitUntilFinish();
    }
}
