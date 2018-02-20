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

import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.nexmark.model.sql.RowSize;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Executor for Nexmark queries. Allows SQL PTransform to be
 * decoupled from Nexmark KnownSize model transform.
 */
public abstract class NexmarkSqlTransform extends PTransform<PCollection<Event>, PCollection<Row>> {
  protected NexmarkSqlTransform(String name) {
    super(name);
  }

  public PCollection<? extends KnownSize> applyModel(PCollection<Row> rows) {
    return rows
        .apply(RowSize.parDo())
        .setCoder(RowSize.CODER);
  }
}
