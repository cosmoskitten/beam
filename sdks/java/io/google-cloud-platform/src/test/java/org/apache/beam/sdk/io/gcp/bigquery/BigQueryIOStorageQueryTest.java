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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createJobIdToken;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createTempTableReference;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;

import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.Storage.Stream;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TableRowParser;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;

/** Tests for {@link BigQueryIO#readTableRows()} using {@link Method#DIRECT_READ}. */
@RunWith(JUnit4.class)
public class BigQueryIOStorageQueryTest {

  private transient PipelineOptions options;
  private transient TemporaryFolder testFolder = new TemporaryFolder();
  private transient TestPipeline p;

  @Rule
  public final transient TestRule folderThenPipeline =
      new TestRule() {
        @Override
        public Statement apply(Statement base, Description description) {
          // We need to set up the temporary folder, and then set up the TestPipeline based on the
          // chosen folder. Unfortunately, since rule evaluation order is unspecified and unrelated
          // to field order, and is separate from construction, that requires manually creating this
          // TestRule.
          Statement withPipeline =
              new Statement() {
                @Override
                public void evaluate() throws Throwable {
                  options = TestPipeline.testingPipelineOptions();
                  options.as(BigQueryOptions.class).setProject("project-id");
                  options
                      .as(BigQueryOptions.class)
                      .setTempLocation(testFolder.getRoot().getAbsolutePath());
                  p = TestPipeline.fromOptions(options);
                  p.apply(base, description).evaluate();
                }
              };

          return testFolder.apply(withPipeline, description);
        }
      };

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private FakeDatasetService fakeDatasetService = new FakeDatasetService();
  private FakeJobService fakeJobService = new FakeJobService();

  private FakeBigQueryServices fakeBigQueryServices =
      new FakeBigQueryServices()
          .withDatasetService(fakeDatasetService)
          .withJobService(fakeJobService);

  @Before
  public void setUp() throws Exception {
    FakeDatasetService.setUp();
  }

  private static final String DEFAULT_QUERY = "SELECT * FROM `dataset.table` LIMIT 1";

  @Test
  public void testDefaultQueryBasedSource() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead();
    checkTypedReadQueryObject(typedRead, DEFAULT_QUERY);
    assertTrue(typedRead.getValidate());
    assertTrue(typedRead.getFlattenResults());
    assertTrue(typedRead.getUseLegacySql());
    assertNull(typedRead.getQueryPriority());
    assertNull(typedRead.getQueryLocation());
    assertNull(typedRead.getKmsKey());
    assertFalse(typedRead.getWithTemplateCompatibility());
  }

  @Test
  public void testQueryBasedSourceWithCustomQuery() throws Exception {
    TypedRead<TableRow> typedRead =
        BigQueryIO.read(new TableRowParser())
            .fromQuery("SELECT * FROM `google.com:project.dataset.table`")
            .withCoder(TableRowJsonCoder.of());
    checkTypedReadQueryObject(typedRead, "SELECT * FROM `google.com:project.dataset.table`");
  }

  @Test
  public void testQueryBasedSourceWithoutValidation() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead().withoutValidation();
    checkTypedReadQueryObject(typedRead, DEFAULT_QUERY);
    assertFalse(typedRead.getValidate());
  }

  @Test
  public void testQueryBasedSourceWithoutResultFlattening() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead().withoutResultFlattening();
    checkTypedReadQueryObject(typedRead, DEFAULT_QUERY);
    assertFalse(typedRead.getFlattenResults());
  }

  @Test
  public void testQueryBasedSourceWithStandardSql() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead().usingStandardSql();
    checkTypedReadQueryObject(typedRead, DEFAULT_QUERY);
    assertFalse(typedRead.getUseLegacySql());
  }

  @Test
  public void testQueryBasedSourceWithPriority() throws Exception {
    TypedRead<TableRow> typedRead =
        getDefaultTypedRead().withQueryPriority(QueryPriority.INTERACTIVE);
    checkTypedReadQueryObject(typedRead, DEFAULT_QUERY);
    assertEquals(QueryPriority.INTERACTIVE, typedRead.getQueryPriority());
  }

  @Test
  public void testQueryBasedSourceWithQueryLocation() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead().withQueryLocation("US");
    checkTypedReadQueryObject(typedRead, DEFAULT_QUERY);
    assertEquals("US", typedRead.getQueryLocation());
  }

  @Test
  public void testQueryBasedSourceWithKmsKey() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead().withKmsKey("kms_key");
    checkTypedReadQueryObject(typedRead, DEFAULT_QUERY);
    assertEquals("kms_key", typedRead.getKmsKey());
  }

  private TypedRead<TableRow> getDefaultTypedRead() {
    return BigQueryIO.read(new TableRowParser())
        .fromQuery(DEFAULT_QUERY)
        .withCoder(TableRowJsonCoder.of())
        .withMethod(Method.DIRECT_READ);
  }

  private void checkTypedReadQueryObject(TypedRead typedRead, String query) {
    assertNull(typedRead.getTable());
    assertEquals(query, typedRead.getQuery().get());
  }

  @Test
  public void testBuildQueryBasedSourceWithReadOptions() throws Exception {
    TableReadOptions readOptions = TableReadOptions.newBuilder().setRowRestriction("a > 5").build();
    TypedRead<TableRow> typedRead = getDefaultTypedRead().withReadOptions(readOptions);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid BigQueryIO.Read: Specifies table read options, "
            + "which only applies when reading from a table");
    p.apply(typedRead);
    p.run();
  }

  @Test
  public void testBuildQueryBasedSourceWithTemplateCompatibility() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead().withTemplateCompatibility();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid BigQueryIO.Read: Specifies template compatibility, "
            + "which is not supported with Method.DIRECT_READ");
    p.apply(typedRead);
    p.run();
  }

  @Test
  public void testDisplayData() throws Exception {
    TypedRead<TableRow> typedRead = getDefaultTypedRead();
    DisplayData displayData = DisplayData.from(typedRead);
    assertThat(displayData, hasDisplayItem("query", DEFAULT_QUERY));
  }

  @Test
  public void testEvaluatedDisplayData() throws Exception {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    TypedRead<TableRow> typedRead = getDefaultTypedRead();
    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveSourceTransforms(typedRead);
    assertThat(displayData, hasItem(hasDisplayItem("query")));
  }

  @Test
  public void testName() {
    assertEquals("BigQueryIO.TypedRead", getDefaultTypedRead().getName());
  }

  @Test
  public void testCoderInference() {
    SerializableFunction<SchemaAndRecord, KV<ByteString, ReadSession>> parseFn =
        new SerializableFunction<SchemaAndRecord, KV<ByteString, ReadSession>>() {
          @Override
          public KV<ByteString, ReadSession> apply(SchemaAndRecord input) {
            return null;
          }
        };

    assertEquals(
        KvCoder.of(ByteStringCoder.of(), ProtoCoder.of(ReadSession.class)),
        BigQueryIO.read(parseFn).inferCoder(CoderRegistry.createDefault()));
  }

  @Test
  public void testQuerySourceEstimatedSize() throws Exception {

    String query =
        FakeBigQueryServices.encodeQuery(
            ImmutableList.of(
                new TableRow().set("name", "a").set("number", 1L),
                new TableRow().set("name", "b").set("number", 2L),
                new TableRow().set("name", "c").set("number", 3L),
                new TableRow().set("name", "d").set("number", 4L),
                new TableRow().set("name", "e").set("number", 5L),
                new TableRow().set("name", "f").set("number", 6L)));

    BigQueryOptions options = PipelineOptionsFactory.create().as(BigQueryOptions.class);
    options.setProject("project");

    fakeJobService.expectDryRunQuery(
        options.getProject(),
        query,
        new JobStatistics().setQuery(new JobStatistics2().setTotalBytesProcessed(125L)));

    BigQueryStorageQuerySource<TableRow> querySource =
        BigQueryStorageQuerySource.create(
            /* stepUuid = */ "stepUuid",
            ValueProvider.StaticValueProvider.of(query),
            /* flattenResults = */ true,
            /* useLegacySql = */ true,
            /* priority = */ QueryPriority.INTERACTIVE,
            /* location = */ null,
            /* kmsKey = */ null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            fakeBigQueryServices);

    assertEquals(125L, querySource.getEstimatedSizeBytes(options));
  }

  @Test
  public void testQuerySourceInitialSplit() throws Exception {
    doQuerySourceInitialSplit(1024L, 1024, 50);
  }

  @Test
  public void testQuerySourceInitialSplit_MinSplitCount() throws Exception {
    doQuerySourceInitialSplit(1024L * 1024L, 10, 1);
  }

  @Test
  public void testQuerySourceInitialSplit_MaxSplitCount() throws Exception {
    doQuerySourceInitialSplit(10, 10_000, 200);
  }

  private void doQuerySourceInitialSplit(
      long bundleSize, int requestedStreamCount, int expectedStreamCount) throws Exception {

    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER")));

    String query =
        FakeBigQueryServices.encodeQuery(
            ImmutableList.of(
                new TableRow().set("name", "a").set("number", 1L),
                new TableRow().set("name", "b").set("number", 2L),
                new TableRow().set("name", "c").set("number", 3L),
                new TableRow().set("name", "d").set("number", 4L),
                new TableRow().set("name", "e").set("number", 5L),
                new TableRow().set("name", "f").set("number", 6L)));

    BigQueryOptions options = PipelineOptionsFactory.create().as(BigQueryOptions.class);
    options.setProject("project");

    String stepUuid = "testStepUuid";

    TableReference tempTableReference =
        createTempTableReference(
            options.getProject(), createJobIdToken(options.getJobName(), stepUuid));

    //
    // N.B. Because the fake job service implementation treats the temporary destination table as
    // both the query source (e.g. the table being queried) and the query destination (e.g. the
    // temporary table to which query results are written), it's necessary to pre-create the
    // temporary table -- rather than a source table of some kind -- before executing the query.
    // This could be improved by modifying the query encoding interface above to store both the
    // source table and the resulting rows.
    //

    fakeDatasetService.createDataset(
        tempTableReference.getProjectId(),
        tempTableReference.getDatasetId(),
        /* location = */ "US",
        "Fake plastic tre^D^D^Dtables",
        TimeUnit.HOURS.toMillis(1));

    fakeDatasetService.createTable(
        new Table()
            .setTableReference(tempTableReference)
            .setSchema(tableSchema)
            .setNumBytes(1024L * 1024L));

    fakeJobService.expectDryRunQuery(
        options.getProject(),
        query,
        new JobStatistics()
            .setQuery(
                new JobStatistics2()
                    .setTotalBytesProcessed(1024L * 1024L)
                    .setReferencedTables(ImmutableList.of(tempTableReference))));

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/" + options.getProject())
            .setTableReference(BigQueryHelpers.toTableRefProto(tempTableReference))
            .setRequestedStreams(requestedStreamCount)
            .build();

    ReadSession.Builder builder = ReadSession.newBuilder();
    for (int i = 0; i < expectedStreamCount; i++) {
      builder.addStreams(Stream.newBuilder().setName("stream-" + i));
    }

    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(builder.build());

    BigQueryStorageQuerySource<TableRow> querySource =
        BigQueryStorageQuerySource.create(
            stepUuid,
            ValueProvider.StaticValueProvider.of(query),
            /* flattenResults = */ true,
            /* useLegacySql = */ true,
            /* priority = */ QueryPriority.BATCH,
            /* location = */ null,
            /* kmsKey = */ null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withJobService(fakeJobService)
                .withStorageClient(fakeStorageClient));

    List<? extends BoundedSource<TableRow>> sources = querySource.split(bundleSize, options);
    assertEquals(expectedStreamCount, sources.size());
  }

  @Test
  public void testQuerySourceInitialSplit_EmptyTable() throws Exception {

    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("name").setType("STRING"),
                    new TableFieldSchema().setName("number").setType("INTEGER")));

    String query = FakeBigQueryServices.encodeQuery(ImmutableList.of());

    BigQueryOptions options = PipelineOptionsFactory.create().as(BigQueryOptions.class);
    options.setProject("project");

    String stepUuid = "testStepUuid";

    TableReference tempTableReference =
        createTempTableReference(
            options.getProject(), createJobIdToken(options.getJobName(), stepUuid));

    fakeDatasetService.createDataset(
        tempTableReference.getProjectId(),
        tempTableReference.getDatasetId(),
        /* location = */ "US",
        "Fake plastic tre^D^D^Dtables",
        TimeUnit.HOURS.toMillis(1));

    fakeDatasetService.createTable(
        new Table().setTableReference(tempTableReference).setSchema(tableSchema).setNumBytes(0L));

    fakeJobService.expectDryRunQuery(
        options.getProject(),
        query,
        new JobStatistics()
            .setQuery(
                new JobStatistics2()
                    .setTotalBytesProcessed(1024L * 1024L)
                    .setReferencedTables(ImmutableList.of(tempTableReference))));

    CreateReadSessionRequest expectedRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/" + options.getProject())
            .setTableReference(BigQueryHelpers.toTableRefProto(tempTableReference))
            .setRequestedStreams(10)
            .build();

    ReadSession emptyReadSession = ReadSession.newBuilder().build();
    StorageClient fakeStorageClient = mock(StorageClient.class);
    when(fakeStorageClient.createReadSession(expectedRequest)).thenReturn(emptyReadSession);

    BigQueryStorageQuerySource<TableRow> querySource =
        BigQueryStorageQuerySource.create(
            stepUuid,
            ValueProvider.StaticValueProvider.of(query),
            /* flattenResults = */ true,
            /* useLegacySql = */ true,
            /* priority = */ QueryPriority.BATCH,
            /* location = */ null,
            /* kmsKey = */ null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            new FakeBigQueryServices()
                .withDatasetService(fakeDatasetService)
                .withJobService(fakeJobService)
                .withStorageClient(fakeStorageClient));

    List<? extends BoundedSource<TableRow>> sources = querySource.split(1024L, options);
    assertTrue(sources.isEmpty());
  }

  @Test
  public void testQuerySourceCreateReader() throws Exception {
    BigQueryStorageQuerySource<TableRow> querySource =
        BigQueryStorageQuerySource.create(
            /* stepUuid = */ "testStepUuid",
            ValueProvider.StaticValueProvider.of("SELECT * FROM `dataset.table`"),
            /* flattenResults = */ false,
            /* useLegacySql = */ false,
            /* priority = */ QueryPriority.INTERACTIVE,
            /* location = */ "asia-northeast1",
            /* kmsKey = */ null,
            new TableRowParser(),
            TableRowJsonCoder.of(),
            fakeBigQueryServices);

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("BigQuery query source must be split before reading");
    querySource.createReader(options);
  }

  @Ignore("This can't be implemented yet")
  @Test
  public void testReadFromBigQueryIO() throws Exception {
    // In the end-to-end test scenario, the step UUID is generated by the pipeline, and so the
    // temporary table can't be pre-created ahead of time.
  }
}
