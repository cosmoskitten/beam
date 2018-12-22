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
package org.apache.beam.sdk.io.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.IterableCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

/**
 * Builds an AggregateIterable pipeline using multiple options.
 *
 * @author Ahmed Elhossaini
 */
public class QueryBuilder {
  private Integer limit;
  private List<String> projection;
  private List<BsonDocument> pipeline;
  MongoCollection<Document> collection;
  private String filter;
  private String documentIdStr;
  private ObjectId documentId;

  public static QueryBuilder create(MongoCollection<Document> collection) {
    QueryBuilder builder = new QueryBuilder();
    builder.collection = collection;

    return builder;
  }

  public QueryBuilder withDocumentIdStr(String documentId) {
    this.documentIdStr = documentId;

    return this;
  }

  public QueryBuilder withDocumentId(ObjectId documentId) {
    this.documentId = documentId;

    return this;
  }

  public QueryBuilder withLimit(Integer limit) {
    this.limit = limit;

    return this;
  }

  public QueryBuilder withProjection(List<String> projection) {
    this.projection = projection;

    return this;
  }

  public QueryBuilder withPipeline(List<BsonDocument> pipeline) {
    this.pipeline = pipeline;

    return this;
  }

  public QueryBuilder withFilter(String filter) {
    this.filter = filter;

    return this;
  }

  public MongoCursor<Document> cursor() {
    List<BsonDocument> mongoDbPipeline = null;
    // A custom pipeline is supplied
    if (this.pipeline != null && !this.pipeline.isEmpty()) {
      mongoDbPipeline = pipeline;
    } else {
      // Build a new pipeline using provided builder options
      List<Bson> aggregates = new ArrayList<Bson>();

      // Set match filters
      if (this.documentIdStr != null) {
        aggregates.add(Aggregates.match(Filters.eq("_id", this.documentIdStr)));
      } else if (this.documentId != null) {
        aggregates.add(Aggregates.match(Filters.eq("_id", this.documentId)));
      } else if (this.filter != null && !this.filter.isEmpty()) {
        aggregates.add(Aggregates.match(BsonDocument.parse(this.filter)));
      }

      // Set projection
      if (this.projection != null) {
        aggregates.add(Aggregates.project(Projections.include(this.projection)));
      }

      // Set limit
      if (this.limit != null && this.limit > 0) {
        aggregates.add(Aggregates.limit(this.limit));
      }

      // Create pipeline stages and register needed condecs
      mongoDbPipeline =
          aggregates
              .stream()
              .map(
                  s ->
                      s.toBsonDocument(
                          BasicDBObject.class,
                          CodecRegistries.fromProviders(
                              new BsonValueCodecProvider(),
                              new ValueCodecProvider(),
                              new IterableCodecProvider())))
              .collect(Collectors.toList());
    }

    return collection.aggregate(mongoDbPipeline).iterator();
  }
}
