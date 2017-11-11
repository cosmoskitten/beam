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

package org.apache.beam.sdk.nexmark.queries.sql;

import org.apache.beam.sdk.coders.BeamRecordCoder;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.nexmark.model.sql.BeamRecordSize;
import org.apache.beam.sdk.nexmark.model.sql.ToBeamRecord;
import org.apache.beam.sdk.nexmark.model.sql.adapter.ModelFieldsAdapters;
import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.PCollection;

/**
 * Query 0: Pass events through unchanged.
 *
 * <p>This measures the overhead of the Beam SQL implementation and test harness like
 * conversion from Java model classes to Beam records.
 *
 * <p>{@link Bid} events are used here at the moment, ås they are most numerous
 * with default configuration.
 */
public class SqlQuery0 extends NexmarkQuery {

  private static final BeamSql.SimpleQueryTransform QUERY =
      BeamSql.query("SELECT * FROM PCOLLECTION");

  public SqlQuery0(NexmarkConfiguration configuration) {
    super(configuration, "SqlQuery0");
  }

  @Override
  protected PCollection<KnownSize> applyPrim(PCollection<Event> allEvents) {
    BeamRecordCoder bidRecordCoder = getBidRecordCoder();

    PCollection<BeamRecord> bidEventsRecords = allEvents
        .apply(Filter.by(IS_BID))
        .apply(ToBeamRecord.parDo())
        .setCoder(bidRecordCoder);

    PCollection<BeamRecord> queryResultsRecords = bidEventsRecords
        .apply(QUERY)
        .setCoder(bidRecordCoder);

    PCollection<? extends KnownSize> bidRecordSizes = queryResultsRecords
            .apply(BeamRecordSize.parDo())
            .setCoder(BeamRecordSize.CODER);

    return NexmarkUtils.castToKnownSize(name, bidRecordSizes);
  }

  private BeamRecordCoder getBidRecordCoder() {
    return ModelFieldsAdapters.ADAPTERS.get(Bid.class).getRecordType().getRecordCoder();
  }
}
