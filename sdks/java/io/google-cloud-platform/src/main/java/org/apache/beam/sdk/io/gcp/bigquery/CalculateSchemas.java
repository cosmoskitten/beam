package org.apache.beam.sdk.io.gcp.bigquery;

/**
 * Created by relax on 4/30/17.
 */
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

import com.google.api.services.bigquery.model.TableRow;
import java.util.Map;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;

import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Compute the mapping of destinations to json-formatted schema objects.
 */
class CalculateSchemas<DestinationT> extends PTransform<PCollection<KV<DestinationT, TableRow>>,
    PCollectionView<Map<DestinationT, String>>> {
  private static final Logger LOG = LoggerFactory.getLogger(CalculateSchemas.class);

  private final DynamicDestinations<?, DestinationT> dynamicDestinations;

  public CalculateSchemas(DynamicDestinations<?, DestinationT> dynamicDestinations) {
    this.dynamicDestinations = dynamicDestinations;
  }

  @Override
  public PCollectionView<Map<DestinationT, String>> expand(
      PCollection<KV<DestinationT, TableRow>> input) {
    return input
        .apply("Keys", Keys.<DestinationT>create())
        .apply("Distinct Keys", Distinct.<DestinationT>create())
        .apply("GetSchemas", ParDo.of(new DoFn<DestinationT, KV<DestinationT, String>>() {
          private DynamicDestinations<?, DestinationT> dynamicDestinationsCopy;

          @StartBundle
          public void startBundle(Context c) {
            // Ensure that each bundle has a fresh copy of the DynamicDestinations class.
            this.dynamicDestinationsCopy = SerializableUtils.clone(dynamicDestinations);
          }

          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            // If the DynamicDestinations class wants to read a side input, give it the value.
            if (dynamicDestinationsCopy.getSideInput() != null) {
              dynamicDestinationsCopy.setSideInputValue(
                  c.sideInput(dynamicDestinationsCopy.getSideInput()));
            }
            c.output(KV.of(c.element(),
                BigQueryHelpers.toJsonString(dynamicDestinationsCopy.getSchema(c.element()))));
          }
        }))
        .apply("asMap", View.<DestinationT, String>asMap());
  }
}

