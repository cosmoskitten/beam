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

import static org.apache.beam.sdk.io.elasticsearch.common.ElasticsearchIOCore.BoundedElasticsearchSource;
import static org.apache.beam.sdk.io.elasticsearch.common.ElasticsearchIOCore.ConnectionConfiguration;
import static org.apache.beam.sdk.io.elasticsearch.common.ElasticsearchIOCore.Read;
import static org.apache.beam.sdk.io.elasticsearch.common.ElasticsearchIOCore.Write;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
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
  private static ElasticsearchRequesterV5 requester = new ElasticsearchRequesterV5();

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

  private static class ElasticsearchRequesterV5 implements ElasticsearchRequester {

    @Override
    public int expectedVersion() {
      return 5;
    }

    @Override
    public JsonNode getErrorNode(JsonNode item) {
      return item.path("index");
    }

    @Override
    public List<BoundedElasticsearchSource> split(long desiredBundleSizeBytes, Read spec)
        throws Exception {
      List<BoundedElasticsearchSource> sources = new ArrayList<>();
      long indexSize = BoundedElasticsearchSource
          .estimateIndexSize(spec.getConnectionConfiguration());
      float nbBundlesFloat = (float) indexSize / desiredBundleSizeBytes;
      int nbBundles = (int) Math.ceil(nbBundlesFloat);
      //ES slice api imposes that the number of slices is <= 1024 even if it can be overloaded
      if (nbBundles > 1024) {
        nbBundles = 1024;
      }
      // split the index into nbBundles chunks of desiredBundleSizeBytes by creating
      // nbBundles sources each reading a slice of the index
      // (see https://goo.gl/MhtSWz)
      // the slice API allows to split the ES shards
      // to have bundles closer to desiredBundleSizeBytes
        for (int i = 0; i < nbBundles; i++) {
          sources.add(new BoundedElasticsearchSource(spec, null, nbBundles, i));
        }
      return sources;
    }

    @Override public String updateReaderStartQuery(String query,
        BoundedElasticsearchSource source) {

      //if source split is uninitialized (like in testSplit() initialSource)
      // or if there is only one slice
      if (source.getNumSlices() == null || source.getNumSlices() == 1) {
        return query;
      }
        // add slice to the user query
      String sliceQuery = String.format("\"slice\": {\"id\": %d,\"max\": %d}", source.getSliceId(),
          source.getNumSlices());
      String outputQuery = query.replaceFirst("\\{", "{" + sliceQuery + ",");
      return outputQuery;
    }

    @Override public Map<String, String> updateReaderStartParams(Map<String, String> params,
        BoundedElasticsearchSource source) {
      // nothing to add to the default params;
      return params;
    }
  }
}
