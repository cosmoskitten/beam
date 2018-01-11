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
package org.apache.beam.examples.snippets;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Code snippets used in webdocs.
 */
public class Snippets {

  @DefaultCoder(AvroCoder.class)
  static class Quote {
    final String source;
    final String quote;

    public Quote() {
      this.source = "";
      this.quote = "";
    }
    public Quote(String source, String quote) {
      this.source = source;
      this.quote = quote;
    }
  }

  /** Using a Read and Write transform to read/write from/to BigQuery. */
  public static void modelBigQueryIO(Pipeline p) {
    modelBigQueryIO(p, "", "", "");
  }

  public static void modelBigQueryIO(Pipeline p,
      String writeProject, String writeDataset, String writeTable) {
    {
      // [START BigQueryTableSpec]
      String tableSpec = "clouddataflow-readonly:samples.weather_stations";
      // [END BigQueryTableSpec]
    }

    {
      // [START BigQueryTableSpecWithoutProject]
      String tableSpec = "samples.weather_stations";
      // [END BigQueryTableSpecWithoutProject]
    }

    {
      // [START BigQueryTableSpecObject]
      TableReference tableSpec = new TableReference()
          .setProjectId("clouddataflow-readonly")
          .setDatasetId("samples")
          .setTableId("weather_stations");
      // [END BigQueryTableSpecObject]
    }

    {
      String tableSpec = "clouddataflow-readonly:samples.weather_stations";
      // [START BigQueryReadTable]
      PCollection<Double> maxTemperatures = p
          .apply(BigQueryIO.readTableRows().from(tableSpec))
          // Each row is of type TableRow
          .apply(MapElements.into(TypeDescriptors.doubles()).via(
              (TableRow row) -> (Double) row.get("max_temperature")));
      // [END BigQueryReadTable]
    }

    {
      // [START BigQueryReadQuery]
      PCollection<Double> maxTemperatures = p
          .apply(BigQueryIO.readTableRows().fromQuery(
              "SELECT max_temperature FROM [clouddataflow-readonly:samples.weather_stations]"))
          // Each row is of type TableRow
          .apply(MapElements.into(TypeDescriptors.doubles()).via(
              (TableRow row) -> (Double) row.get("max_temperature")));
      // [END BigQueryReadQuery]
    }

    {
      // [START BigQueryReadQueryStdSQL]
      PCollection<Double> maxTemperatures = p
          .apply(BigQueryIO.readTableRows().fromQuery(
              "SELECT max_temperature FROM `clouddataflow-readonly.samples.weather_stations`")
              .usingStandardSql())
          // Each row is of type TableRow
          .apply(MapElements.into(TypeDescriptors.doubles()).via(
              (TableRow row) -> (Double) row.get("max_temperature")));
      // [END BigQueryReadQueryStdSQL]
    }

    {
      // [START BigQuerySchemaJson]
      String tableSchemaJson = ""
          + "{"
          + "  \"fields\": ["
          + "    {"
          + "      \"name\": \"source\","
          + "      \"type\": \"STRING\","
          + "      \"mode\": \"NULLABLE\""
          + "    },"
          + "    {"
          + "      \"name\": \"quote\","
          + "      \"type\": \"STRING\","
          + "      \"mode\": \"REQUIRED\""
          + "    }"
          + "  ]"
          + "}";
      // [END BigQuerySchemaJson]
    }

    // [START BigQuerySchemaObject]
    List<TableFieldSchema> fields = new ArrayList<>(Arrays.asList(
        new TableFieldSchema().setName("source").setType("STRING").setMode("NULLABLE"),
        new TableFieldSchema().setName("quote").setType("STRING").setMode("REQUIRED")));
    TableSchema tableSchema = new TableSchema().setFields(fields);
    // [END BigQuerySchemaObject]

    {
      String tableSpec = "clouddataflow-readonly:samples.weather_stations";
      if (!writeProject.isEmpty() && !writeDataset.isEmpty() && !writeTable.isEmpty()) {
        tableSpec = writeProject + ":" + writeDataset + "." + writeTable;
      }

      // [START BigQueryWrite]
      /*
      @DefaultCoder(AvroCoder.class)
      static class Quote {
        final String source;
        final String quote;

        public Quote() {
          this.source = "";
          this.quote = "";
        }
        public Quote(String source, String quote) {
          this.source = source;
          this.quote = quote;
        }
      }
      */

      PCollection<Quote> quotes = p.apply(Create.of(
          new Quote("Mahatma Ghandi", "My life is my message.")
      ));

      quotes
          .apply(MapElements.into(TypeDescriptor.of(TableRow.class)).via(
              (Quote elem) -> new TableRow().set("source", elem.source).set("quote", elem.quote)
          ))
          .apply(BigQueryIO.writeTableRows()
              .to(tableSpec)
              .withSchema(tableSchema)
              .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
              .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));
      // [END BigQueryWrite]
    }
  }

  /** Helper function to format results in coGroupByKeyTuple. */
  public static String formatCoGbkResults(
      String name, Iterable<String> emails, Iterable<String> phones) {

    List<String> emailsList = new ArrayList<>();
    for (String elem : emails) {
      emailsList.add("'" + elem + "'");
    }
    Collections.sort(emailsList);
    String emailsStr = "[" + String.join(", ", emailsList) + "]";

    List<String> phonesList = new ArrayList<>();
    for (String elem : phones) {
      phonesList.add("'" + elem + "'");
    }
    Collections.sort(phonesList);
    String phonesStr = "[" + String.join(", ", phonesList) + "]";

    return name + "; " + emailsStr + "; " + phonesStr;
  }

  /** Using a CoGroupByKey transform. */
  public static PCollection<String> coGroupByKeyTuple(
      TupleTag<String> emailsTag,
      TupleTag<String> phonesTag,
      PCollection<KV<String, String>> emails,
      PCollection<KV<String, String>> phones) {

    // [START CoGroupByKeyTuple]
    PCollection<KV<String, CoGbkResult>> results =
        KeyedPCollectionTuple
        .of(emailsTag, emails)
        .and(phonesTag, phones)
        .apply(CoGroupByKey.create());

    PCollection<String> contactLines = results.apply(ParDo.of(
      new DoFn<KV<String, CoGbkResult>, String>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          KV<String, CoGbkResult> e = c.element();
          String name = e.getKey();
          Iterable<String> emailsIter = e.getValue().getAll(emailsTag);
          Iterable<String> phonesIter = e.getValue().getAll(phonesTag);
          String formattedResult = Snippets.formatCoGbkResults(name, emailsIter, phonesIter);
          c.output(formattedResult);
        }
      }
    ));
    // [END CoGroupByKeyTuple]
    return contactLines;
  }
}
