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
package org.apache.beam.dsls.sql.schema;

import java.io.Serializable;

/**
 * abstract class of aggregation functions in Beam SQL.
 *
 * <p>There're several constrains for a UDAF:<br>
 * 1. A constructor with an empty argument list is required;<br>
 * 2. The type of {@code InputT} and {@code OutputT} can only be Interger/Long/Short/Byte/Double
 * /Float/Date, mapping as SQL type INTEGER/BIGINT/SMALLINT/TINYINE/DOUBLE/FLOAT/TIMESTAMP;<br>
 * 3. wrap intermediate data in a {@link BeamSqlRow}, and do not rely on elements in class;<br>
 */
public abstract class BeamSqlUdaf<InputT, OutputT> implements Serializable {
  public BeamSqlUdaf(){}

  public abstract BeamSqlRow init();

  public abstract BeamSqlRow add(BeamSqlRow accumulator, InputT input);

  public abstract BeamSqlRow merge(Iterable<BeamSqlRow> accumulators);

  public abstract OutputT result(BeamSqlRow accumulator);
}
