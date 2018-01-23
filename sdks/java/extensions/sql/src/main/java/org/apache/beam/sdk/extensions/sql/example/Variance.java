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
package org.apache.beam.sdk.extensions.sql.example;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamRecordSqlType;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.BeamTextCSVTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.BeamTextCSVTableIOReader;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a quick example, which uses Beam SQL DSL to create a data pipeline.
 *
 * <p>Run the example with
 * <pre>
 * mvn -pl sdks/java/extensions/sql \
 *   compile exec:java -Dexec.mainClass=org.apache.beam.sdk.extensions.sql.example.BeamSqlExample \
 *   -Dexec.args="--runner=DirectRunner" -Pdirect-runner
 * </pre>
 */


class Variance {
    private static final Logger LOG = LoggerFactory
            .getLogger(BeamTextCSVTable.class);

    public interface VarianceOptions extends PipelineOptions {

        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("/Users/jiangkai/Downloads/taxi.csv")
        String getInputFile();
        void setInputFile(String value);

        /**
         * Set this required option to specify where to write the output.
         */
        @Description("Path of the file to write to")
        @Default.String("/tmp/taxi")
        @Validation.Required
        String getOutput();
        void setOutput(String value);
    }

    public static void main(String[] args) throws Exception {
        VarianceOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(VarianceOptions.class);
        Pipeline p = Pipeline.create(options);


        List<String> fieldNames1 = Arrays.asList("taxi_id", "trip_start_timestamp",
                "trip_end_timestamp",
                "trip_seconds", "trip_miles", "pickup_census_tract", "dropoff_census_tract",
                "pickup_community_area",
                "dropoff_community_area", "fare", "tips", "tolls", "extras", "trip_total",
                "payment_type", "company",
                "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude");
        List<Integer> fieldTypes1 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.DOUBLE, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.DOUBLE, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR);
        BeamRecordSqlType type1 = BeamRecordSqlType.create(fieldNames1, fieldTypes1);
        CSVFormat priceCSV = CSVFormat.EXCEL.withHeader("taxi_id", "trip_start_timestamp",
                "trip_end_timestamp",
                "trip_seconds", "trip_miles", "pickup_census_tract", "dropoff_census_tract",
                "pickup_community_area",
                "dropoff_community_area", "fare", "tips", "tolls", "extras", "trip_total",
                "payment_type", "company",
                "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude")
                .withAllowMissingColumnNames().withDelimiter(',').withIgnoreEmptyLines();
        PCollection<BeamRecord> priceTable = PBegin.in(p).apply(TextIO.read().
                from(options.getInputFile()))
                .apply(new BeamTextCSVTableIOReader(type1, options.getInputFile(), priceCSV))
                .setCoder(type1.getRecordCoder());

        PCollection<BeamRecord> outputStream3 = priceTable.apply(
                BeamSql.query("select covar_samp(trip_miles, trip_total), "
                        + "var_samp(trip_total) from PCOLLECTION"));

        outputStream3.apply("price",
                MapElements.via(
                new SimpleFunction<BeamRecord, String>() {
                    public @Nullable String apply(BeamRecord input) {
                        System.out.println("PCOLLECTION: " + input.getDataValues());
                        return "PCOLLECTION: " + input.getDataValues();
                    }
                })).apply(TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
    }
}
