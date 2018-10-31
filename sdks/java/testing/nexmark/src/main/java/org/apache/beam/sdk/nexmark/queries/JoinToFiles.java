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
package org.apache.beam.sdk.nexmark.queries;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Query 1, 'Currency Conversion'. Convert each bid value from dollars to euros. In CQL syntax:
 *
 * <pre>
 * SELECT Istream(auction, DOLTOEUR(price), bidder, datetime)
 * FROM bid [ROWS UNBOUNDED];
 * </pre>
 *
 * <p>To make things more interesting, allow the 'currency conversion' to be arbitrarily slowed
 * down.
 */
public class JoinToFiles extends NexmarkQuery {
  public JoinToFiles(NexmarkConfiguration configuration) {
    super(configuration, "JoinToFiles");
  }

  private PCollectionView<Map<Long, String>> getSideInputMap(Pipeline p) {
    return p.apply(TextIO.read().from(configuration.sideInputUrl + "*"))
        .apply(
            MapElements.via(
                new SimpleFunction<String, KV<Long, String>>(
                    line -> {
                      List<String> cols = ImmutableList.copyOf(Splitter.on(",").split(line));
                      return KV.of(Long.valueOf(cols.get(0)), cols.get(1));
                    }) {}))
        .apply(View.asMap());
  }

  private PCollection<Bid> applyTyped(PCollection<Event> events) {

    final PCollectionView<Map<Long, String>> sideInputMap = getSideInputMap(events.getPipeline());

    return events
        // Only want the bid events; easier to fake some side input data
        .apply(JUST_BIDS)

        // Map the conversion function over all bids.
        .apply(
            name + ".JoinToFiles",
            ParDo.of(
                    new DoFn<Bid, Bid>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        Bid bid = c.element();
                        c.output(
                            new Bid(
                                bid.auction,
                                bid.bidder,
                                (bid.price * 89) / 100,
                                bid.dateTime,
                                c.sideInput(sideInputMap)
                                    .get(bid.bidder % configuration.sideInputRowCount)));
                      }
                    })
                .withSideInputs(sideInputMap));
  }

  @Override
  protected PCollection<KnownSize> applyPrim(PCollection<Event> events) {
    return NexmarkUtils.castToKnownSize(name, applyTyped(events));
  }
}
