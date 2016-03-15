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

package org.apache.beam.runners.gearpump.translators;

import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.runners.gearpump.GearpumpPipelineRunner;
import org.apache.beam.runners.gearpump.translators.utils.TestUtils;
import org.apache.beam.runners.gearpump.translators.utils.Verify;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.cluster.embedded.EmbeddedCluster;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * integration test for {@link ParDoBoundMultiTranslator}.
 */
public class ParDoBoundMultiTranslatorTest {

  @Test
  public void test() throws Exception {
    GearpumpPipelineOptions options = PipelineOptionsFactory.create()
        .as(GearpumpPipelineOptions.class);
    options.setApplicationName("ParDoBoundMulti");
    options.setRunner(GearpumpPipelineRunner.class);

    EmbeddedCluster cluster = EmbeddedCluster.apply();
    cluster.start();
    options.setEmbeddedCluster(cluster);

    Pipeline pipeline = Pipeline.create(options);

    int number = 100;
    List<Integer> collection = new ArrayList<>(number);
    for (int i = 0; i < number; i++) {
      collection.add(i);
    }

    int partitionNum = 10;
    TupleTagList outputTags = TupleTagList.empty();
    for (int i = 0; i < partitionNum; i++) {
      outputTags = outputTags.and(new TupleTag<Integer>("" + i));
    }
    PCollectionList<Integer> pcs = pipeline.apply(Create.of(collection))
        .apply(PartitionBy.of(outputTags, new PartitionFn()));

    for (TupleTag<?> tag : outputTags.getAll()) {
      int partition = Integer.parseInt(tag.getId());
      List<Integer> expected = collection.subList(partition * partitionNum, partition *
          partitionNum + 10);
      pcs.get(partition).apply(ParDo.of(new Verify<>(expected)));
    }

    pipeline.run();

    Thread.sleep(1000);
    ClientContext clientContext = options.getClientContext();
    TestUtils.checkFailure(clientContext);

    clientContext.close();
    cluster.stop();
  }

  /**
   * like {@link Partition} except for user generated output tags.
   */
  private static class PartitionFn implements PartitionBy.Fn<Integer> {

    @Override
    public int partitionFor(Integer elem, int numPartitions) {
      return elem / numPartitions;
    }
  }

  private static class PartitionBy<T> extends PTransform<PCollection<T>, PCollectionList<T>> {

    private interface Fn<T> extends Serializable {
      int partitionFor(T elem, int numPartitions);
    }

    public static <T> PartitionBy<T> of(TupleTagList outputTags, Fn<? super T> partitionFn) {
      return new PartitionBy<>(new PartitionDoFn<>(outputTags, partitionFn));
    }

    @Override
    public PCollectionList<T> apply(PCollection<T> in) {
      final TupleTagList outputTags = partitionDoFn.getOutputTags();

      PCollectionTuple outputs = in.apply(
          ParDo
              .withOutputTags(new TupleTag<Void>() {
              }, outputTags)
              .of(partitionDoFn));

      PCollectionList<T> pcs = PCollectionList.empty(in.getPipeline());
      Coder<T> coder = in.getCoder();

      for (TupleTag<?> outputTag : outputTags.getAll()) {
        // All the tuple tags are actually TupleTag<T>
        // And all the collections are actually PCollection<T>
        @SuppressWarnings("unchecked")
        TupleTag<T> typedOutputTag = (TupleTag<T>) outputTag;
        pcs = pcs.and(outputs.get(typedOutputTag).setCoder(coder));
      }
      return pcs;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.include(partitionDoFn);
    }

    private final transient PartitionDoFn<T> partitionDoFn;

    private PartitionBy(PartitionDoFn<T> partitionDoFn) {
      this.partitionDoFn = partitionDoFn;
    }

    private static class PartitionDoFn<X> extends DoFn<X, Void> {
      private final TupleTagList outputTags;
      private final Fn<? super X> partitionFn;
      private final int numPartitions;

      public PartitionDoFn(TupleTagList outputTags, Fn<? super X> partitionFn) {
        this.outputTags = outputTags;
        if (outputTags.size() == 0) {
          throw new IllegalArgumentException("tag list can't be empty");
        }
        this.numPartitions = outputTags.size();
        this.partitionFn = partitionFn;
      }

      public TupleTagList getOutputTags() {
        return outputTags;
      }

      @Override
      public void processElement(ProcessContext c) {
        X input = c.element();
        int partition = partitionFn.partitionFor(input, numPartitions);
        if (0 <= partition && partition < numPartitions) {
          @SuppressWarnings("unchecked")
          TupleTag<X> typedTag = (TupleTag<X>) outputTags.get(partition);
          c.sideOutput(typedTag, input);
        } else {
          throw new IndexOutOfBoundsException(
              "Partition function returned out of bounds index: " +
                  partition + " not in [0.." + numPartitions + ")");
        }
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder
            .add(DisplayData.item("numPartitions", numPartitions))
            .add(DisplayData.item("partitionFn", partitionFn.getClass()));
      }
    }
  }

}
