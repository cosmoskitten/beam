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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.sdk.io.elasticsearch.common.ElasticsearchIOCore.BoundedElasticsearchSource;
import static org.apache.beam.sdk.io.elasticsearch.common.ElasticsearchIOCore.ConnectionConfiguration;
import static org.apache.beam.sdk.io.elasticsearch.common.ElasticsearchIOCore.Read;
import static org.apache.beam.sdk.io.elasticsearch.common.ElasticsearchIOCore.Write;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.elasticsearch.common.ElasticsearchIOCore;
import org.apache.beam.sdk.io.elasticsearch.common.ElasticsearchRequester;
import org.apache.beam.sdk.values.PCollection;

/**
 * Transforms for reading and writing data from/to Elasticsearch.
 * This IO is only compatible with Elasticsearch v2.x
 *
 * <h3>Reading from Elasticsearch</h3>
 *
 * <p>{@link ElasticsearchIO#read ElasticsearchIO.read()} returns a bounded
 * {@link PCollection PCollection&lt;String&gt;} representing JSON documents.
 *
 * <p>To configure the {@link ElasticsearchIO#read}, you have to provide a connection configuration
 * containing the HTTP address of the instances, an index name and a type. The following example
 * illustrates options for configuring the source:
 *
 * <pre>{@code
 *
 * pipeline.apply(ElasticsearchIO.read().withConnectionConfiguration(
 *    ElasticsearchIO.ConnectionConfiguration.create("http://host:9200", "my-index", "my-type")
 * )
 *
 * }</pre>
 *
 * <p>The connection configuration also accepts optional configuration: {@code withUsername()} and
 * {@code withPassword()}.
 *
 * <p>You can also specify a query on the {@code read()} using {@code withQuery()}.
 *
 * <p>Optionally, you can provide {@code withBatchSize()} to specify the size of batch reads
 * in number of documents or {@code withScrollKeepalive()} to specify the scroll context keepalive
 *
 * <h3>Writing to Elasticsearch</h3>
 *
 * <p>To write documents to Elasticsearch, use
 * {@link ElasticsearchIO#write ElasticsearchIO.write()}, which writes JSON documents from a
 * {@link PCollection PCollection&lt;String&gt;} (which can be bounded or unbounded).
 *
 * <p>To configure {@link ElasticsearchIO#write ElasticsearchIO.write()}, similar to the read, you
 * have to provide a connection configuration. For instance:
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(...)
 *    .apply(ElasticsearchIO.write().withConnectionConfiguration(
 *       ElasticsearchIO.createConfiguration("http://host:9200", "my-index", "my-type")
 *    )
 *
 * }</pre>
 *
 * <p>Optionally, you can provide {@code withMaxBatchSize()} and {@code withMaxBatchSizeBytes()}
 * to specify the size of the write batch in number of documents or in bytes.
 */
public class ElasticsearchIO {

  //Cannot extend ElasticsearchIOCore because of uses of static. Use composition instead
  private static ElasticsearchRequesterV2 requester = new ElasticsearchRequesterV2();

  public static ConnectionConfiguration createConfiguration(String[] addresses, String index,
      String type) {
    ElasticsearchIOCore.setRequester(requester);
    return ConnectionConfiguration.create(addresses, index, type);

  }
  public static Read read(){
    ElasticsearchIOCore.setRequester(requester);
    return ElasticsearchIOCore.read();
  }

  public static Write write(){
    ElasticsearchIOCore.setRequester(requester);
    return ElasticsearchIOCore.write();
  }

  private static class ElasticsearchRequesterV2 implements ElasticsearchRequester {

    @Override public int expectedVersion() {
      return 2;
    }

    @Override public JsonNode getErrorNode(JsonNode item) {
      return item.path("create");
    }

    @Override public List<BoundedElasticsearchSource> split(long desiredBundleSizeBytes, Read spec)
        throws Exception {
      List<BoundedElasticsearchSource> sources = new ArrayList<>();
      // 1. We split per shard :
      // unfortunately, Elasticsearch 2. x doesn 't provide a way to do parallel reads on a single
      // shard.So we do not use desiredBundleSize because we cannot split shards.
      // With the slice API in ES 5.0 we will be able to use desiredBundleSize.
      // Basically we will just ask the slice API to return data
      // in nbBundles = estimatedSize / desiredBundleSize chuncks.
      // So each beam source will read around desiredBundleSize volume of data.

      ConnectionConfiguration connectionConfiguration = spec.getConnectionConfiguration();
      JsonNode statsJson = BoundedElasticsearchSource.getStats(connectionConfiguration, true);
      JsonNode shardsJson =
          statsJson
              .path("indices")
              .path(connectionConfiguration.getIndex())
              .path("shards");

      Iterator<Map.Entry<String, JsonNode>> shards = shardsJson.fields();
      while (shards.hasNext()) {
        Map.Entry<String, JsonNode> shardJson = shards.next();
        String shardId = shardJson.getKey();
        sources.add(new BoundedElasticsearchSource(spec, shardId, null, null));
      }
      checkArgument(!sources.isEmpty(), "No shard found");
      return sources;
    }

    @Override public String updateReaderStartQuery(String query,
        BoundedElasticsearchSource source) {
      // nothing to add to the user query
      return query;
    }

    @Override public Map<String, String> updateReaderStartParams(Map<String, String> params,
        BoundedElasticsearchSource source) {
      // do not modify input parameters
      Map<String, String> outputParams = new HashMap<>(params);
      outputParams.put("size", String.valueOf(source.getSpec().getBatchSize()));
      if (source.getShardPreference() != null) {
        outputParams.put("preference", "_shards:" + source.getShardPreference());
      }
      return outputParams;
    }
  }
}
