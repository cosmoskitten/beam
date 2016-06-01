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
package org.apache.beam.runners.direct;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.apache.avro.reflect.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.UUID;

/**
 * A special test for {@link GroupByKey} that verifies that a runner uses
 * the encoded form of a value for hashing/comparison.
 */
@RunWith(JUnit4.class)
public class InProcessGroupByKeyTest {

  /**
   * This is a bogus key class that returns random hash values from {@link #hashCode()} and always
   * returns {@code false} for {@link #equals(Object)}. The results are correct if Flink does not
   * use this for hashing/sorting.
   */
  @DefaultCoder(AvroCoder.class)
  static class Key {
    @Nullable
    String key1;
    @Nullable
    Long key2;

    public Key() {}

    public Key(String s, Long k) {
      key1 = s;
      key2 = k;
    }

    @Override
    public boolean equals(Object o) {
      return false;
    }

    @Override
    public int hashCode() {
      UUID uuid = UUID.randomUUID();
      return uuid.hashCode();
    }
  }

  @DefaultCoder(AvroCoder.class)
  public static class DumbData {
    @Nullable
    String key1;
    @Nullable
    Long key2;
    @Nullable
    Long value1;

    public DumbData() {
    }
  }

  @DefaultCoder(AvroCoder.class)
  static class Config {
    int key1;
    int key2;
    int perKey;
    long val;

    public Config() {
    }

    public Config(int k1, int k2, int pk, long v) {
      key1 = k1;
      key2 = k2;
      perKey = pk;
      val = v;
    }
  }

  static class MakeKey extends SimpleFunction<DumbData, Key> {

    @Override
    public Key apply(DumbData dd) {
      return new Key(dd.key1, dd.key2);
    }
  }

  static class Generator extends DoFn<Config, DumbData> {

    @Override
    public void processElement(ProcessContext c) throws Exception {
      Config cfg = c.element();

      ArrayList<String> bs = new ArrayList<>(cfg.key1);
      for (int i = 0; i < cfg.key1; i++) {
        bs.add("hereGoesLongStringID" + i);
      }

      for (int k = 0; k < cfg.perKey; k++) {
        for (long j = 0; j < cfg.key2; j++) {
          for (int i = 0; i < cfg.key1; i++) {
            DumbData dd = new DumbData();
            dd.key1 = bs.get(i);
            dd.key2 = j;
            dd.value1 = cfg.val;
            c.output(dd);
          }
        }
      }
    }
  }

  public static class MergeGbk extends DoFn<KV<Key, Iterable<DumbData>>, String> {

    private static final Logger LOG = LoggerFactory.getLogger(MergeGbk.class);

    private final Aggregator<Long, Long> keyCnt =
        createAggregator("key count", new Sum.SumLongFn());

    private final Aggregator<Long, Long> itemCnt =
        createAggregator("item count", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) throws Exception {
      Key key = c.element().getKey();
      Iterable<DumbData> data = c.element().getValue();

      keyCnt.addValue(1L);
      long count = 0;
      for (DumbData val : data) {
        count++;
      }
      itemCnt.addValue(count);

      c.output(key.key1 + "," + key.key2 + "," + count);
      if (count == 0) {
        LOG.info("no data for (" + key.key1 + "," + key.key2 + ")");
      } else {
        LOG.info(count + " data items for (" + key.key1 + "," + key.key2 + ")");
      }
    }
  }

  @Test
  public void testGroupByKeyWithLargeData() throws Exception {
    final int numValues = 10;

    PipelineOptions options = PipelineOptionsFactory.as(PipelineOptions.class);

    // we need at least parallelism 2 so that we actually have a shuffle
//    options.setParallelism(2);
    options.setRunner(InProcessPipelineRunner.class);

    Pipeline p = TestPipeline.fromOptions(options);

    // Create several Configs, so that several parallel Generator instances emit
    // data. Otherwise, a pre-shuffle combine would already combine everything
    // and results would be correct even with a bogus key.hashCode().
    PCollection<KV<Key, DumbData>> dataset1 = p
        .apply(Create.of(
            new Config(3, 5, numValues, 2),
            new Config(3, 5, numValues, 2),
            new Config(3, 5, numValues, 2),
            new Config(3, 5, numValues, 2)))
        .apply(ParDo.of(new Generator()))
        .apply(WithKeys.of(new MakeKey()));

    PCollection<String> result = dataset1
        .apply(GroupByKey.<Key, DumbData>create())
        .apply(ParDo.of(new MergeGbk()));

    PAssert.that(result).containsInAnyOrder(
        "hereGoesLongStringID0,0," + (numValues * 4),
        "hereGoesLongStringID0,1," + (numValues * 4),
        "hereGoesLongStringID0,2," + (numValues * 4),
        "hereGoesLongStringID0,3," + (numValues * 4),
        "hereGoesLongStringID0,4," + (numValues * 4),
        "hereGoesLongStringID1,0," + (numValues * 4),
        "hereGoesLongStringID1,1," + (numValues * 4),
        "hereGoesLongStringID1,2," + (numValues * 4),
        "hereGoesLongStringID1,3," + (numValues * 4),
        "hereGoesLongStringID1,4," + (numValues * 4),
        "hereGoesLongStringID2,0," + (numValues * 4),
        "hereGoesLongStringID2,1," + (numValues * 4),
        "hereGoesLongStringID2,2," + (numValues * 4),
        "hereGoesLongStringID2,3," + (numValues * 4),
        "hereGoesLongStringID2,4," + (numValues * 4)
    );

    p.run();
  }
}
