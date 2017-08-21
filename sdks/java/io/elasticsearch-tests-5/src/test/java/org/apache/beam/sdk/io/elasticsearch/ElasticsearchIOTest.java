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
package org.apache.beam.sdk.io.elasticsearch;

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.BoundedElasticsearchSource;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Read;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.isA;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.PCollection;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.hamcrest.CustomMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/*
Cannot use @RunWith(JUnit4.class) with ESIntegTestCase
Cannot have @Before[class] @After[class] with ESIntegTestCase
*/
/** Tests for {@link ElasticsearchIO}. */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ElasticsearchIOTest extends ESIntegTestCase implements Serializable {

  private static final String ES_INDEX = "beam";
  private static final String ES_TYPE = "test";
  private static final long NUM_DOCS = 400L;
  private static final int NUM_SCIENTISTS = 10;
  private static final long BATCH_SIZE = 200L;
  private static final long AVERAGE_DOC_SIZE = 15L;
  private static final long BATCH_SIZE_BYTES = 2048L;

  private String[] fillAddresses(){
    ArrayList<String> result = new ArrayList<>();
    for (InetSocketAddress address : cluster().httpAddresses()){
      result.add(String.format("http://%s:%d", address.getHostString(), address.getPort()));
    }
    return result.toArray(new String[result.size()]);
  }

  @Override
  protected Settings nodeSettings(int nodeOrdinal) {
    return Settings.builder().put(super.nodeSettings(nodeOrdinal))
        .put("http.enabled", "true")
        // had problems with some jdk, embedded ES was too slow for bulk insertion,
        // and queue of 50 was full. No pb with real ES instance (cf testWrite integration test)
        .put("thread_pool.bulk.queue_size", 100)
        .build();
  }

  @Override public Settings indexSettings() {
    return Settings.builder().put(super.indexSettings())
        //useful to have updated sizes for getEstimatedSize
        .put("index.store.stats_refresh_interval", 0)
        .build();
  }


  @Override
  protected Collection<Class<? extends Plugin>> nodePlugins() {
    ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>();
    plugins.add(Netty4Plugin.class);
    return plugins;
  }

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  //TODO initialize ConnectionConfiguration only once
  @Test
  public void testSizes() throws Exception {
    ConnectionConfiguration connectionConfiguration =
        ConnectionConfiguration.create(fillAddresses(), ES_INDEX, ES_TYPE);
    //need to create the index using the helper method (not create it at first insertion)
    // for the indexSettings() to be run
    createIndex(ES_INDEX);
    ElasticSearchIOTestUtils.insertTestDocuments(ES_INDEX, ES_TYPE, NUM_DOCS, getRestClient());
    PipelineOptions options = PipelineOptionsFactory.create();
    Read read =
        ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration);
    BoundedElasticsearchSource initialSource = new BoundedElasticsearchSource(read, null, null,
        null, 5);
    // can't use equal assert as Elasticsearch indexes never have same size
    // (due to internal Elasticsearch implementation)
    long estimatedSize = initialSource.getEstimatedSizeBytes(options);
    logger.info("Estimated size: {}", estimatedSize);
    assertThat("Wrong estimated size", estimatedSize, greaterThan(AVERAGE_DOC_SIZE * NUM_DOCS));
  }

 @Test
  public void testRead() throws Exception {
   ConnectionConfiguration connectionConfiguration =
       ConnectionConfiguration.create(fillAddresses(), ES_INDEX, ES_TYPE);
   //need to create the index using the helper method (not create it at first insertion)
   // for the indexSettings() to be run
   createIndex(ES_INDEX);
   ElasticSearchIOTestUtils.insertTestDocuments(ES_INDEX, ES_TYPE, NUM_DOCS, getRestClient());

    PCollection<String> output =
        pipeline.apply(
            ElasticsearchIO.read()
                .withConnectionConfiguration(connectionConfiguration)
                //set to default value, useful just to test parameter passing.
                .withScrollKeepalive("5m")
                //set to default value, useful just to test parameter passing.
                .withBatchSize(100L));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(NUM_DOCS);
    pipeline.run();
  }

 @Test
  public void testReadWithQuery() throws Exception {
   ConnectionConfiguration connectionConfiguration =
       ConnectionConfiguration.create(fillAddresses(), ES_INDEX, ES_TYPE);
   //need to create the index using the helper method (not create it at first insertion)
   // for the indexSettings() to be run
   createIndex(ES_INDEX);
   ElasticSearchIOTestUtils.insertTestDocuments(ES_INDEX, ES_TYPE, NUM_DOCS, getRestClient());

    String query =
        "{\n"
            + "  \"query\": {\n"
            + "  \"match\" : {\n"
            + "    \"scientist\" : {\n"
            + "      \"query\" : \"Einstein\",\n"
            + "      \"type\" : \"boolean\"\n"
            + "    }\n"
            + "  }\n"
            + "  }\n"
            + "}";

    PCollection<String> output =
        pipeline.apply(
            ElasticsearchIO.read()
                .withConnectionConfiguration(connectionConfiguration)
                .withQuery(query));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally()))
        .isEqualTo(NUM_DOCS / NUM_SCIENTISTS);
    pipeline.run();
  }

 @Test
  public void testWrite() throws Exception {
   ConnectionConfiguration connectionConfiguration =
       ConnectionConfiguration.create(fillAddresses(), ES_INDEX, ES_TYPE);
   List<String> data =
        ElasticSearchIOTestUtils.createDocuments(
            NUM_DOCS, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    pipeline
        .apply(Create.of(data))
        .apply(ElasticsearchIO.write().withConnectionConfiguration(connectionConfiguration));
    pipeline.run();

    long currentNumDocs = ElasticSearchIOTestUtils
        .refreshIndexAndGetCurrentNumDocs(ES_INDEX, ES_TYPE, getRestClient());
   assertEquals(NUM_DOCS, currentNumDocs);

    QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("Einstein").field("scientist");
    SearchResponse searchResponse =
        client()
            .prepareSearch(ES_INDEX)
            .setTypes(ES_TYPE)
            .setQuery(queryBuilder)
            .execute()
            .actionGet();
    assertEquals(NUM_DOCS / NUM_SCIENTISTS, searchResponse.getHits().getTotalHits());
  }

  @Rule public ExpectedException exception = ExpectedException.none();

 @Test
  public void testWriteWithErrors() throws Exception {
   ConnectionConfiguration connectionConfiguration =
       ConnectionConfiguration.create(fillAddresses(), ES_INDEX, ES_TYPE);
   Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSize(BATCH_SIZE);
    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    DoFnTester<String, Void> fnTester = DoFnTester.of(new Write.WriteFn(write, 5));

    List<String> input =
        ElasticSearchIOTestUtils.createDocuments(
            NUM_DOCS, ElasticSearchIOTestUtils.InjectionMode.INJECT_SOME_INVALID_DOCS);
    exception.expect(isA(IOException.class));
    exception.expectMessage(
        new CustomMatcher<String>("RegExp matcher") {
          @Override
          public boolean matches(Object o) {
            String message = (String) o;
            // This regexp tests that 2 malformed documents are actually in error
            // and that the message contains their IDs.
            // It also ensures that root reason, root error type,
            // caused by reason and caused by error type are present in message.
            // To avoid flakiness of the test in case of Elasticsearch error message change,
            // only "failed to parse" root reason is matched,
            // the other messages are matched using .+
            return message.matches(
                "(?is).*Error writing to Elasticsearch, some elements could not be inserted"
                    + ".*Document id .+: failed to parse \\(.+\\).*Caused by: .+ \\(.+\\).*"
                    + "Document id .+: failed to parse \\(.+\\).*Caused by: .+ \\(.+\\).*");
          }
        });
    // inserts into Elasticsearch
    fnTester.processBundle(input);
  }

 @Test
  public void testWriteWithMaxBatchSize() throws Exception {
   ConnectionConfiguration connectionConfiguration =
       ConnectionConfiguration.create(fillAddresses(), ES_INDEX, ES_TYPE);
   Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSize(BATCH_SIZE);
    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    DoFnTester<String, Void> fnTester = DoFnTester.of(new Write.WriteFn(write, 5));
    List<String> input =
        ElasticSearchIOTestUtils.createDocuments(
            NUM_DOCS, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    long numDocsProcessed = 0;
    long numDocsInserted = 0;
    for (String document : input) {
      fnTester.processElement(document);
      numDocsProcessed++;
      // test every 100 docs to avoid overloading ES
      if ((numDocsProcessed % 100) == 0) {
        // force the index to upgrade after inserting for the inserted docs
        // to be searchable immediately
        long currentNumDocs = ElasticSearchIOTestUtils
            .refreshIndexAndGetCurrentNumDocs(ES_INDEX, ES_TYPE, getRestClient());
        if ((numDocsProcessed % BATCH_SIZE) == 0) {
          /* bundle end */
          assertEquals(
              "we are at the end of a bundle, we should have inserted all processed documents",
              numDocsProcessed,
              currentNumDocs);
          numDocsInserted = currentNumDocs;
        } else {
          /* not bundle end */
          assertEquals(
              "we are not at the end of a bundle, we should have inserted no more documents",
              numDocsInserted,
              currentNumDocs);
        }
      }
    }
  }

 @Test
  public void testWriteWithMaxBatchSizeBytes() throws Exception {
   ConnectionConfiguration connectionConfiguration =
       ConnectionConfiguration.create(fillAddresses(), ES_INDEX, ES_TYPE);
   Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSizeBytes(BATCH_SIZE_BYTES);
    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    DoFnTester<String, Void> fnTester = DoFnTester.of(new Write.WriteFn(write, 5));
    List<String> input =
        ElasticSearchIOTestUtils.createDocuments(
            NUM_DOCS, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    long numDocsProcessed = 0;
    long sizeProcessed = 0;
    long numDocsInserted = 0;
    long batchInserted = 0;
    for (String document : input) {
      fnTester.processElement(document);
      numDocsProcessed++;
      sizeProcessed += document.getBytes().length;
      // test every 40 docs to avoid overloading ES
      if ((numDocsProcessed % 40) == 0) {
        // force the index to upgrade after inserting for the inserted docs
        // to be searchable immediately
        long currentNumDocs =
            ElasticSearchIOTestUtils.refreshIndexAndGetCurrentNumDocs(
                ES_INDEX, ES_TYPE, getRestClient());
        if (sizeProcessed / BATCH_SIZE_BYTES > batchInserted) {
          /* bundle end */
          assertThat(
              "we have passed a bundle size, we should have inserted some documents",
              currentNumDocs,
              greaterThan(numDocsInserted));
          numDocsInserted = currentNumDocs;
          batchInserted = (sizeProcessed / BATCH_SIZE_BYTES);
        } else {
          /* not bundle end */
          assertEquals(
              "we are not at the end of a bundle, we should have inserted no more documents",
              numDocsInserted,
              currentNumDocs);
        }
      }
    }
  }

 @Test
  public void testSplit() throws Exception {
   ConnectionConfiguration connectionConfiguration =
       ConnectionConfiguration.create(fillAddresses(), ES_INDEX, ES_TYPE);
   //need to create the index using the helper method (not create it at first insertion)
   // for the indexSettings() to be run
   createIndex(ES_INDEX);
   ElasticSearchIOTestUtils.insertTestDocuments(ES_INDEX, ES_TYPE, NUM_DOCS, getRestClient());
    PipelineOptions options = PipelineOptionsFactory.create();
    Read read =
        ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration);
   BoundedElasticsearchSource initialSource = new BoundedElasticsearchSource(read, null, null,
       null, 5);
   int desiredBundleSizeBytes = 1000;
    List<? extends BoundedSource<String>> splits =
        initialSource.split(desiredBundleSizeBytes, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(initialSource, splits, options);
   long indexSize = BoundedElasticsearchSource.estimateIndexSize(connectionConfiguration);
   float expectedNumSourcesFloat = (float) indexSize / desiredBundleSizeBytes;
   int expectedNumSources = (int) Math.ceil(expectedNumSourcesFloat);
   assertEquals(expectedNumSources, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<String> subSource : splits) {
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    }
    assertEquals("Wrong number of empty splits", expectedNumSources, nonEmptySplits);
  }
}
