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
package org.apache.beam.sdk.io.elasticsearch.common;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

/**
 * Core of ElasticsearchIO (common code for all the versions).
 * This class should not be used by directly.
 */
public class ElasticsearchIOCore {

  private static ElasticsearchRequester requester;

  public static void setRequester(ElasticsearchRequester r){
    requester = r;
  }

  public static Read read() {
    // default scrollKeepalive = 5m as a majorant for un-predictable time between 2 start/read calls
    // default batchSize to 100 as recommended by ES dev team as a safe value when dealing
    // with big documents and still a good compromise for performances
    return new AutoValue_ElasticsearchIOCore_Read.Builder()
        .setScrollKeepalive("5m")
        .setBatchSize(100L)
        .build();
  }

  public static Write write() {
    return new AutoValue_ElasticsearchIOCore_Write.Builder()
        // advised default starting batch size in ES docs
        .setMaxBatchSize(1000L)
        // advised default starting batch size in ES docs
        .setMaxBatchSizeBytes(5L * 1024L * 1024L)
        .build();
  }

  private ElasticsearchIOCore() {}

  private static final ObjectMapper mapper = new ObjectMapper();

  public static JsonNode parseResponse(Response response) throws IOException {
    return mapper.readValue(response.getEntity().getContent(), JsonNode.class);
  }

  /** A POJO describing a connection configuration to Elasticsearch. */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    public abstract List<String> getAddresses();

    @Nullable
    public abstract String getUsername();

    @Nullable
    public abstract String getPassword();

    @Nullable
    public abstract String getKeystorePath();

    @Nullable
    public abstract String getKeystorePassword();

    public abstract String getIndex();

    public abstract String getType();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setAddresses(List<String> addresses);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract Builder setKeystorePath(String keystorePath);

      abstract Builder setKeystorePassword(String password);

      abstract Builder setIndex(String index);

      abstract Builder setType(String type);

      abstract ConnectionConfiguration build();
    }

    /**
     * Creates a new Elasticsearch connection configuration.
     *
     * @param addresses list of addresses of Elasticsearch nodes
     * @param index the index toward which the requests will be issued
     * @param type the document type toward which the requests will be issued
     * @return the connection configuration object
     */
    public static ConnectionConfiguration create(String[] addresses, String index, String type){
      checkArgument(
          addresses != null,
          "ConnectionConfiguration.create(addresses, index, type) called with null address");
      checkArgument(
          addresses.length != 0,
          "ConnectionConfiguration.create(addresses, "
              + "index, type) called with empty addresses");
      checkArgument(
          index != null,
          "ConnectionConfiguration.create(addresses, index, type) called with null index");
      checkArgument(
          type != null,
          "ConnectionConfiguration.create(addresses, index, type) called with null type");
      ConnectionConfiguration connectionConfiguration =
          new AutoValue_ElasticsearchIOCore_ConnectionConfiguration.Builder()
              .setAddresses(Arrays.asList(addresses))
              .setIndex(index)
              .setType(type)
              .build();
      return connectionConfiguration;
    }

    /**
     * If Elasticsearch authentication is enabled, provide the username.
     *
     * @param username the username used to authenticate to Elasticsearch
     * @return the {@link ConnectionConfiguration} object with username set
     */
    public ConnectionConfiguration withUsername(String username) {
      checkArgument(
          username != null,
          "ConnectionConfiguration.create().withUsername(username) called with null username");
      checkArgument(
          !username.isEmpty(),
          "ConnectionConfiguration.create().withUsername(username) called with empty username");
      return builder().setUsername(username).build();
    }

    /**
     * If Elasticsearch authentication is enabled, provide the password.
     *
     * @param password the password used to authenticate to Elasticsearch
     * @return the {@link ConnectionConfiguration} object with password set
     */
    public ConnectionConfiguration withPassword(String password) {
      checkArgument(
          password != null,
          "ConnectionConfiguration.create().withPassword(password) called with null password");
      checkArgument(
          !password.isEmpty(),
          "ConnectionConfiguration.create().withPassword(password) called with empty password");
      return builder().setPassword(password).build();
    }

    /**
     * If Elasticsearch uses SSL/TLS with mutual authentication (via shield),
     * provide the keystore containing the client key.
     *
     * @param keystorePath the location of the keystore containing the client key.
     * @return the {@link ConnectionConfiguration} object with keystore path set.
     */
    public ConnectionConfiguration withKeystorePath(String keystorePath) {
      checkArgument(keystorePath != null, "ConnectionConfiguration.create()"
          + ".withKeystorePath(keystorePath) called with null keystorePath");
      checkArgument(!keystorePath.isEmpty(), "ConnectionConfiguration.create()"
          + ".withKeystorePath(keystorePath) called with empty keystorePath");
      return builder().setKeystorePath(keystorePath).build();
    }

    /**
     * If Elasticsearch uses SSL/TLS with mutual authentication (via shield),
     * provide the password to open the client keystore.
     *
     * @param keystorePassword the password of the client keystore.
     * @return the {@link ConnectionConfiguration} object with keystore passwo:rd set.
     */
    public ConnectionConfiguration withKeystorePassword(String keystorePassword) {
        checkArgument(keystorePassword != null, "ConnectionConfiguration.create()"
            + ".withKeystorePassword(keystorePassword) called with null keystorePassword");
        return builder().setKeystorePassword(keystorePassword).build();
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("address", getAddresses().toString()));
      builder.add(DisplayData.item("index", getIndex()));
      builder.add(DisplayData.item("type", getType()));
      builder.addIfNotNull(DisplayData.item("username", getUsername()));
      builder.addIfNotNull(DisplayData.item("keystore.path", getKeystorePath()));
    }

    public RestClient createClient() throws IOException {
      HttpHost[] hosts = new HttpHost[getAddresses().size()];
      int i = 0;
      for (String address : getAddresses()) {
        URL url = new URL(address);
        hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
        i++;
      }
      RestClientBuilder restClientBuilder = RestClient.builder(hosts);
      if (getUsername() != null) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
            AuthScope.ANY, new UsernamePasswordCredentials(getUsername(), getPassword()));
        restClientBuilder.setHttpClientConfigCallback(
            new RestClientBuilder.HttpClientConfigCallback() {
              public HttpAsyncClientBuilder customizeHttpClient(
                  HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
              }
            });
      }
      if (getKeystorePath() != null && !getKeystorePath().isEmpty()) {
        try {
          KeyStore keyStore = KeyStore.getInstance("jks");
          try (InputStream is = new FileInputStream(new File(getKeystorePath()))) {
            keyStore.load(is, getKeystorePassword().toCharArray());
          }
          final SSLContext sslContext = SSLContexts.custom()
              .loadTrustMaterial(keyStore, new TrustSelfSignedStrategy()).build();
          final SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslContext);
          restClientBuilder.setHttpClientConfigCallback(
              new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(
                HttpAsyncClientBuilder httpClientBuilder) {
              return httpClientBuilder.setSSLContext(sslContext).setSSLStrategy(sessionStrategy);
            }
          });
        } catch (Exception e) {
          throw new IOException("Can't load the client certificate from the keystore", e);
        }
      }
      return restClientBuilder.build();
    }
  }

  /** A {@link PTransform} reading data from Elasticsearch. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    private static final long MAX_BATCH_SIZE = 10000L;

    @Nullable
    public abstract ConnectionConfiguration getConnectionConfiguration();

    @Nullable
    public abstract String getQuery();

    public abstract String getScrollKeepalive();

    public abstract long getBatchSize();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setQuery(String query);

      abstract Builder setScrollKeepalive(String scrollKeepalive);

      abstract Builder setBatchSize(long batchSize);

      abstract Read build();
    }

    /**
     * Provide the Elasticsearch connection configuration object.
     *
     * @param connectionConfiguration the Elasticsearch {@link ConnectionConfiguration} object
     * @return the {@link Read} with connection configuration set
     */
    public Read withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(
          connectionConfiguration != null,
          "ElasticsearchIO.read()"
              + ".withConnectionConfiguration(configuration) called with null configuration");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide a query used while reading from Elasticsearch.
     *
     * @param query the query. See <a
     *     href="https://www.elastic.co/guide/en/elasticsearch/reference/2.4/query-dsl.html">Query
     *     DSL</a>
     * @return the {@link Read} object with query set
     */
    public Read withQuery(String query) {
      checkArgument(
          !Strings.isNullOrEmpty(query),
          "ElasticsearchIO.read().withQuery(query) called" + " with null or empty query");
      return builder().setQuery(query).build();
    }

    /**
     * Provide a scroll keepalive. See <a
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-request-scroll.html">scroll
     * API</a> Default is "5m". Change this only if you get "No search context found" errors.
     *
     * @param scrollKeepalive keepalive duration ex "5m" from 5 minutes
     * @return the {@link Read} with scroll keepalive set
     */
    public Read withScrollKeepalive(String scrollKeepalive) {
      checkArgument(
          scrollKeepalive != null && !scrollKeepalive.equals("0m"),
          "ElasticsearchIO.read().withScrollKeepalive(keepalive) called"
              + " with null or \"0m\" keepalive");
      return builder().setScrollKeepalive(scrollKeepalive).build();
    }

    /**
     * Provide a size for the scroll read. See <a
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/2.4/search-request-scroll.html">
     * scroll API</a> Default is 100. Maximum is 10 000. If documents are small, increasing batch
     * size might improve read performance. If documents are big, you might need to decrease
     * batchSize
     *
     * @param batchSize number of documents read in each scroll read
     * @return the {@link Read} with batch size set
     */
    public Read withBatchSize(long batchSize) {
      checkArgument(
          batchSize > 0,
          "ElasticsearchIO.read().withBatchSize(batchSize) called with a negative "
              + "or equal to 0 value: %s",
          batchSize);
      checkArgument(
          batchSize <= MAX_BATCH_SIZE,
          "ElasticsearchIO.read().withBatchSize(batchSize) "
              + "called with a too large value (over %s): %s",
          MAX_BATCH_SIZE,
          batchSize);
      return builder().setBatchSize(batchSize).build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      return input.apply(
          org.apache.beam.sdk.io.Read.from(new BoundedElasticsearchSource(this, null, null, null)));
    }

    @Override
    public void validate(PipelineOptions options) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      checkState(
          connectionConfiguration != null,
          "ElasticsearchIO.read() requires a connection configuration"
              + " to be set via withConnectionConfiguration(configuration)");
      checkVersion(connectionConfiguration);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("query", getQuery()));
      builder.addIfNotNull(DisplayData.item("batchSize", getBatchSize()));
      builder.addIfNotNull(DisplayData.item("scrollKeepalive", getScrollKeepalive()));
      getConnectionConfiguration().populateDisplayData(builder);
    }
  }

  /** A {@link BoundedSource} reading from Elasticsearch. */
  @VisibleForTesting
  public static class BoundedElasticsearchSource extends BoundedSource<String> {

    private final Read spec;
    // shardPreference is the shard id where the source will read the documents
    @Nullable
    private final String shardPreference;
    @Nullable
    private final Integer numSlices;
    @Nullable
    private final Integer sliceId;

    public Read getSpec() {
      return spec;
    }

    @Nullable public String getShardPreference() {
      return shardPreference;
    }

    @Nullable public Integer getNumSlices() {
      return numSlices;
    }

    @Nullable public Integer getSliceId() {
      return sliceId;
    }

    public BoundedElasticsearchSource(Read spec, @Nullable String shardPreference,
        @Nullable Integer numSlices, @Nullable Integer sliceId) {
      this.spec = spec;
      this.shardPreference = shardPreference;
      this.numSlices = numSlices;
      this.sliceId = sliceId;
    }

    @Override
    public List<? extends BoundedSource<String>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return requester.split(desiredBundleSizeBytes, spec);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      return estimateIndexSize(spec.getConnectionConfiguration());
      }

    public static long estimateIndexSize(ConnectionConfiguration connectionConfiguration)
        throws IOException {
      // we use indices stats API to estimate size and list the shards
      // (https://www.elastic.co/guide/en/elasticsearch/reference/2.4/indices-stats.html)
      // as Elasticsearch 2.x doesn't not support any way to do parallel read inside a shard
      // the estimated size bytes is not really used in the split into bundles.
      // However, we implement this method anyway as the runners can use it.
      // NB: Elasticsearch 5.x now provides the slice API.
      // (https://www.elastic.co/guide/en/elasticsearch/reference/5.0/search-request-scroll.html
      // #sliced-scroll)
      JsonNode statsJson = getStats(connectionConfiguration, false);
      JsonNode indexStats =
          statsJson
              .path("indices")
              .path(connectionConfiguration.getIndex())
              .path("primaries");
      JsonNode store = indexStats.path("store");
      return store.path("size_in_bytes").asLong();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("shard", shardPreference));
      builder.addIfNotNull(DisplayData.item("numSlices", numSlices));
      builder.addIfNotNull(DisplayData.item("sliceId", sliceId));
    }

    @Override
    public BoundedReader<String> createReader(PipelineOptions options) throws IOException {
      return new BoundedElasticsearchReader(this);
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public Coder<String> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }

    public static JsonNode getStats(ConnectionConfiguration connectionConfiguration,
        boolean shardLevel) throws IOException {
      HashMap<String, String> params = new HashMap<>();
      if (shardLevel) {
        params.put("level", "shards");
      }
      String endpoint = String.format("/%s/_stats", connectionConfiguration.getIndex());
      try (RestClient restClient = connectionConfiguration.createClient()) {
        return parseResponse(
            restClient.performRequest("GET", endpoint, params));
      }
    }
  }

  private static class BoundedElasticsearchReader extends BoundedSource.BoundedReader<String> {

    private final BoundedElasticsearchSource source;

    private RestClient restClient;
    private String current;
    private String scrollId;
    private ListIterator<String> batchIterator;

    private BoundedElasticsearchReader(BoundedElasticsearchSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      restClient = source.spec.getConnectionConfiguration().createClient();

      String query = source.spec.getQuery();
      if (query == null) {
        query = "{\"query\": { \"match_all\": {} }}";
      }
      query = requester.updateReaderStartQuery(query, source);

      Response response;
      String endPoint =
          String.format(
              "/%s/%s/_search",
              source.spec.getConnectionConfiguration().getIndex(),
              source.spec.getConnectionConfiguration().getType());
      Map<String, String> params = new HashMap<>();
      params.put("scroll", source.spec.getScrollKeepalive());
      params = requester.updateReaderStartParams(params, source);
      HttpEntity queryEntity = new NStringEntity(query,
          ContentType.APPLICATION_JSON);
      response =
          restClient.performRequest("GET", endPoint, params, queryEntity);
      JsonNode searchResult = parseResponse(response);
      updateScrollId(searchResult);
      return readNextBatchAndReturnFirstDocument(searchResult);
    }

    private void updateScrollId(JsonNode searchResult) {
      scrollId = searchResult.path("_scroll_id").asText();
    }

    @Override
    public boolean advance() throws IOException {
      if (batchIterator.hasNext()) {
        current = batchIterator.next();
        return true;
      } else {
        String requestBody =
            String.format(
                "{\"scroll\" : \"%s\",\"scroll_id\" : \"%s\"}",
                source.spec.getScrollKeepalive(), scrollId);
        HttpEntity scrollEntity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
        Response response =
            restClient.performRequest(
                "GET",
                "/_search/scroll",
                Collections.<String, String>emptyMap(),
                scrollEntity);
        JsonNode searchResult = parseResponse(response);
        updateScrollId(searchResult);
        return readNextBatchAndReturnFirstDocument(searchResult);
      }
    }

    private boolean readNextBatchAndReturnFirstDocument(JsonNode searchResult) {
      //stop if no more data
      JsonNode hits = searchResult.path("hits").path("hits");
      if (hits.size() == 0) {
        current = null;
        batchIterator = null;
        return false;
      }
      // list behind iterator is empty
      List<String> batch = new ArrayList<>();
      for (JsonNode hit : hits) {
        String document = hit.path("_source").toString();
        batch.add(document);
      }
      batchIterator = batch.listIterator();
      current = batchIterator.next();
      return true;
    }

    @Override
    public String getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public void close() throws IOException {
      // remove the scroll
      String requestBody = String.format("{\"scroll_id\" : [\"%s\"]}", scrollId);
      HttpEntity entity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
      try {
        restClient.performRequest(
            "DELETE",
            "/_search/scroll",
            Collections.<String, String>emptyMap(),
            entity);
      } finally {
        if (restClient != null) {
          restClient.close();
        }
      }
    }

    @Override
    public BoundedSource<String> getCurrentSource() {
      return source;
    }
  }

  /** A {@link PTransform} writing data to Elasticsearch. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, PDone> {

    @Nullable
    abstract ConnectionConfiguration getConnectionConfiguration();

    abstract long getMaxBatchSize();

    abstract long getMaxBatchSizeBytes();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setMaxBatchSize(long maxBatchSize);

      abstract Builder setMaxBatchSizeBytes(long maxBatchSizeBytes);

      abstract Write build();
    }

    /**
     * Provide the Elasticsearch connection configuration object.
     *
     * @param connectionConfiguration the Elasticsearch {@link ConnectionConfiguration} object
     * @return the {@link Write} with connection configuration set
     */
    public Write withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(
          connectionConfiguration != null,
          "ElasticsearchIO.write()"
              + ".withConnectionConfiguration(configuration) called with null configuration");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide a maximum size in number of documents for the batch see bulk API
     * (https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-bulk.html). Default is 1000
     * docs (like Elasticsearch bulk size advice). See
     * https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html Depending on the
     * execution engine, size of bundles may vary, this sets the maximum size. Change this if you
     * need to have smaller ElasticSearch bulks.
     *
     * @param batchSize maximum batch size in number of documents
     * @return the {@link Write} with connection batch size set
     */
    public Write withMaxBatchSize(long batchSize) {
      checkArgument(
          batchSize > 0,
          "ElasticsearchIO.write()"
              + ".withMaxBatchSize(batchSize) called with incorrect <= 0 value");
      return builder().setMaxBatchSize(batchSize).build();
    }

    /**
     * Provide a maximum size in bytes for the batch see bulk API
     * (https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-bulk.html). Default is 5MB
     * (like Elasticsearch bulk size advice). See
     * https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html Depending on the
     * execution engine, size of bundles may vary, this sets the maximum size. Change this if you
     * need to have smaller ElasticSearch bulks.
     *
     * @param batchSizeBytes maximum batch size in bytes
     * @return the {@link Write} with connection batch size in bytes set
     */
    public Write withMaxBatchSizeBytes(long batchSizeBytes) {
      checkArgument(
          batchSizeBytes > 0,
          "ElasticsearchIO.write()"
              + ".withMaxBatchSizeBytes(batchSizeBytes) called with incorrect <= 0 value");
      return builder().setMaxBatchSizeBytes(batchSizeBytes).build();
    }

    @Override
    public void validate(PipelineOptions options) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      checkState(
          connectionConfiguration != null,
          "ElasticsearchIO.write() requires a connection configuration"
              + " to be set via withConnectionConfiguration(configuration)");
      checkVersion(connectionConfiguration);
    }

    @Override
    public PDone expand(PCollection<String> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    /**
     * {@link DoFn} to for the {@link Write} transform.
     * */
    @VisibleForTesting
    public static class WriteFn extends DoFn<String, Void> {

      private final Write spec;

      private transient RestClient restClient;
      private ArrayList<String> batch;
      private long currentBatchSizeBytes;

      public WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void createClient() throws Exception {
        restClient = spec.getConnectionConfiguration().createClient();
      }

      @StartBundle
      public void startBundle(StartBundleContext context) throws Exception {
        batch = new ArrayList<>();
        currentBatchSizeBytes = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        String document = context.element();
        batch.add(String.format("{ \"index\" : {} }%n%s%n", document));
        currentBatchSizeBytes += document.getBytes().length;
        if (batch.size() >= spec.getMaxBatchSize()
            || currentBatchSizeBytes >= spec.getMaxBatchSizeBytes()) {
          flushBatch();
        }
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext context) throws Exception {
        flushBatch();
      }

      private void flushBatch() throws IOException {
        if (batch.isEmpty()) {
          return;
        }
        StringBuilder bulkRequest = new StringBuilder();
        for (String json : batch) {
          bulkRequest.append(json);
        }
        batch.clear();
        currentBatchSizeBytes = 0;
        Response response;
        String endPoint =
            String.format(
                "/%s/%s/_bulk",
                spec.getConnectionConfiguration().getIndex(),
                spec.getConnectionConfiguration().getType());
        HttpEntity requestBody =
            new NStringEntity(bulkRequest.toString(), ContentType.APPLICATION_JSON);
        response =
            restClient.performRequest(
                "POST",
                endPoint,
                Collections.<String, String>emptyMap(),
                requestBody);
        JsonNode searchResult = parseResponse(response);
        boolean errors = searchResult.path("errors").asBoolean();
        if (errors) {
          StringBuilder errorMessages =
              new StringBuilder(
                  "Error writing to Elasticsearch, some elements could not be inserted:");
          JsonNode items = searchResult.path("items");
          //some items present in bulk might have errors, concatenate error messages
          for (JsonNode item : items) {
            JsonNode errorRoot = requester.getErrorNode(item);
            JsonNode error = errorRoot.get("error");
            if (error != null) {
              String type = error.path("type").asText();
              String reason = error.path("reason").asText();
              String docId = errorRoot.path("_id").asText();
              errorMessages.append(String.format("%nDocument id %s: %s (%s)", docId, reason, type));
              JsonNode causedBy = error.get("caused_by");
              if (causedBy != null) {
                String cbReason = causedBy.path("reason").asText();
                String cbType = causedBy.path("type").asText();
                errorMessages.append(String.format("%nCaused by: %s (%s)", cbReason, cbType));
              }
            }
          }
          throw new IOException(errorMessages.toString());
        }
      }

      @Teardown
      public void closeClient() throws Exception {
        if (restClient != null) {
          restClient.close();
        }
      }
    }
  }
  private static void checkVersion(ConnectionConfiguration connectionConfiguration){
    int expectedVersion = requester.expectedVersion();
    try (RestClient restClient = connectionConfiguration.createClient()) {
      Response response = restClient.performRequest("GET", "");
      JsonNode jsonNode = parseResponse(response);
      String version = jsonNode.path("version").path("number").asText();
      boolean isVersionCorrect = version.startsWith(String.valueOf(expectedVersion) + ".");
      checkArgument(isVersionCorrect, "The Elasticsearch version to connect to is "
              + "different of %s.x. This version of the ElasticsearchIO is only compatible with "
              + "Elasticsearch v%s.x",
          expectedVersion);
    } catch (IOException e){
      throw (new IllegalArgumentException("Cannot check Elasticsearch version"));
    }
  }
}
