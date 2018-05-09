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
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsub;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamIOType;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

/**
 * <i>Experimental</i>
 *
 * <p>Wraps the {@link PubsubIO} with JSON messages into {@link BeamSqlTable}.
 *
 * <p>This enables {@link PubsubIO} registration in Beam SQL environment as a table, including DDL
 * support.
 *
 * <p>For example:
 * <pre>
 *
 *  CREATE TABLE topic (name VARCHAR, age INTEGER)
 *     TYPE 'pubsub'
 *     LOCATION projects/&lt;GCP project id&gt;/topics/&lt;topic name&gt;
 *     TBLPROPERTIES '{ \"timestampAttributeKey\" : &lt;timestamp attribute&gt; }';
 *
 *   SELECT name, age FROM topic;
 *
 * </pre>
 */
@AutoValue
@Internal
@Experimental
abstract class PubsubIOJsonTable implements BeamSqlTable, Serializable {

  /**
   * Optional attribute key of the Pubsub message from which to extract the event timestamp.
   *
   * <p>This attribute has to conform to the same requirements as in {@link
   * PubsubIO.Read.Builder#withTimestampAttribute}.
   *
   * <p>Short version: it has to be either millis since epoch or string in RFC 3339 format.
   *
   * <p>If the attribute is specified then event timestamps will be extracted from
   * the specified attribute. If it is not specified then message publish timestamp will be used.
   */
  @Nullable abstract String getTimestampAttribute();

  /**
   * Pubsub topic name.
   *
   * <p>Topic is the only way to specify the Pubsub source. Explicitly specifying the subscription
   * is not supported at the moment. Subscriptions are automatically created an managed.
   */
  abstract String getTopic();

  static Builder builder() {
    return new AutoValue_PubsubIOJsonTable.Builder();
  }

  /**
   * Table schema, describes Pubsub message schema.
   *
   * <p>Includes fields 'timestamp', 'attributes, and 'payload'. See {@link PubsubMessageToRow}.
   */
   public abstract Schema getSchema();

  @Override
  public BeamIOType getSourceType() {
    return BeamIOType.UNBOUNDED;
  }

  @Override
  public PCollection<Row> buildIOReader(Pipeline pipeline) {
    return
        PBegin
            .in(pipeline)
            .apply("readFromPubsub", readMessagesWithAttributes())
            .apply("parseJsonPayload", PubsubMessageToRow.forSchema(getSchema()))
            .setCoder(getSchema().getRowCoder());
  }

  private PubsubIO.Read<PubsubMessage> readMessagesWithAttributes() {
    PubsubIO.Read<PubsubMessage> read = PubsubIO
        .readMessagesWithAttributes()
        .fromTopic(getTopic());

    return (getTimestampAttribute() == null)
        ? read
        : read.withTimestampAttribute(getTimestampAttribute());
  }

  @Override
  public PTransform<? super PCollection<Row>, POutput> buildIOWriter() {
    throw new UnsupportedOperationException("Writing to a Pubsub topic is not supported");
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setSchema(Schema schema);
    abstract Builder setTimestampAttribute(String timestampAttribute);
    abstract Builder setTopic(String topic);

    abstract PubsubIOJsonTable build();
  }
}
