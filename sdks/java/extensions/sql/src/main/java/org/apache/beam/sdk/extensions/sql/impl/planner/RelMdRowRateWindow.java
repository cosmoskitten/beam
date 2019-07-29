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
package org.apache.beam.sdk.extensions.sql.impl.planner;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * This is the implementation of RowRateWindowMetadata. Methods to estimate rate and row count for
 * Calcite's logical nodes be implemented here.
 */
public class RelMdRowRateWindow implements MetadataHandler<RowRateWindowMetadata> {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          RowRateWindowMetadata.METHOD, new RelMdRowRateWindow());

  @Override
  public MetadataDef<RowRateWindowMetadata> getDef() {
    return RowRateWindowMetadata.DEF;
  }

  @SuppressWarnings("UnusedDeclaration")
  public RowRateWindow getRowRateWindow(RelNode rel, RelMetadataQuery mq) {

    if (rel instanceof BeamRelNode) {
      return this.getBeamRowRateWindow((BeamRelNode) rel, mq);
    }

    // We can later define custom methods for all different RelNodes to prevent hitting this point.
    // Similar to RelMdRowCount in calcite.

    return RowRateWindow.UNKNOWN;
  }

  private RowRateWindow getBeamRowRateWindow(BeamRelNode rel, RelMetadataQuery mq) {

    // Removing the unknown results.

    List<List> keys =
        mq.map.entrySet().stream()
            .filter(entry -> entry.getValue() instanceof RowRateWindow)
            .filter(entry -> ((RowRateWindow) entry.getValue()).isUnknown())
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

    for (List key : keys) {
      mq.map.remove(key);
    }

    return rel.estimateRowRateWindow(mq);
  }
}
