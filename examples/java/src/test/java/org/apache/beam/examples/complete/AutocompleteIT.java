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
package org.apache.beam.examples.complete;

import org.apache.beam.examples.complete.AutoComplete.Options;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AutocompleteIT {

  public static final String DEFAULT_INPUT = "gs://dataflow-samples/shakespeare/kinglear.txt";

  public static final Long DEFAULT_INPUT_CHECKSUM = 1;


  public interface AutocompleteITOptions extends TestPipelineOptions, Options {}

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  @Test
  public void testE2EAutoComplete() throws Exception {
    AutocompleteITOptions options = TestPipeline.testingPipelineOptions().as(AutocompleteITOptions.class);

    options.setInputFile(DEFAULT_INPUT);
    options.setOutputToBigQuery(false);
    options.setOutputToDatastore(false);
    options.setOutputToChecksum(true);
    options.setExpectedChecksum(DEFAULT_INPUT_CHECKSUM);


    AutoComplete.runAutocompletePipeline(options);
  }

}
