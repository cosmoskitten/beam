/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.examples;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileBasedSink.CompressionType;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Example pipeline showing how to use {@link TextIO.Write} and {@link CompressionType} to write out
 * Gzipped data.
 * <p>
 * This does a word count against the files in {@code gs://dataflow-samples/shakespeare/*} resulting
 * in output records of single {@value <word>,<count>} pairs, with the final output files in both
 * plain text and Gzip compressed format.
 *
 * @see DecoratedFileSink
 * @see WriterOutputGzipDecoratorFactory
 */
public class ShakespeareWordCountToGzipFileSink {
  public static void main(String[] args) {
    // Set any necessary pipeline options.
    final PipelineOptions options = PipelineOptionsFactory.create();
    // Wire together pipeline transforms.
    final Pipeline p = Pipeline.create(options);
    final PCollection<String> wc = p.apply(TextIO.Read.from("gs://dataflow-samples/shakespeare/*"))
        .apply(FlatMapElements.via((String word) -> Arrays.asList(word.split("[^a-zA-Z']+")))
            .withOutputType(new TypeDescriptor<String>() {}))
        .apply(Filter.by((String word) -> !word.isEmpty()))
        .apply(Count.<String>perElement())
        .apply(MapElements
            .via((KV<String, Long> wordCount) -> wordCount.getKey() + "," + wordCount.getValue())
            .withOutputType(new TypeDescriptor<String>() {}));
    wc.apply("WriteGzipToGCS", TextIO.Write.to("<YOUR_GCS_PLAIN_TEXT_OUTPUT_LOCATION>")
        .withSuffix("txt").withOutputWrapperFactory(CompressionType.GZIP));
    // Run pipeline.
    p.run();
  }
}
