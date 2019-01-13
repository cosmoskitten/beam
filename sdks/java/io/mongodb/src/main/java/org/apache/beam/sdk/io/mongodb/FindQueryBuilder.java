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
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Projections;
import java.util.List;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.IterableCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builds a MongoDB FindIterable object. */
public class FindQueryBuilder
    implements SerializableFunction<MongoCollection<Document>, MongoCursor<Document>> {
  private static final Logger LOG = LoggerFactory.getLogger(FindQueryBuilder.class);

  private List<String> projection;
  private Integer limit;
  private BsonDocument filters;
  private String id;

  public FindQueryBuilder withId(String id) {
    this.id = id;
    return this;
  }

  public FindQueryBuilder withLimit(Integer limit) {
    this.limit = limit;
    return this;
  }

  public FindQueryBuilder withProjection(List<String> projection) {
    this.projection = projection;
    return this;
  }

  public FindQueryBuilder withFilters(Bson filters) {
    this.filters =
        filters.toBsonDocument(
            BasicDBObject.class,
            CodecRegistries.fromProviders(
                new BsonValueCodecProvider(),
                new ValueCodecProvider(),
                new IterableCodecProvider()));
    return this;
  }

  @Override
  public MongoCursor<Document> apply(MongoCollection<Document> collection) {

    if (this.id != null && this.filters != null) {
      throw new InvalidParameterException("Cannot set both object ID and filter");
    }

    FindIterable<Document> finder = collection.find();

    if (this.id != null) {
      Document doc = new Document();
      doc.put("_id", new ObjectId(id));
      finder = finder.filter(doc);
    } else if (filters != null) {
      finder = finder.filter(filters);
    }

    // Set limit
    if (limit != null) {
      finder = finder.limit(limit);
    }

    // Set projection
    if (this.projection != null) {
      finder = finder.projection(Projections.include(this.projection));
    }

    return finder.iterator();
  }

  public static FindQueryBuilder create() {
    return new FindQueryBuilder();
  }
}
