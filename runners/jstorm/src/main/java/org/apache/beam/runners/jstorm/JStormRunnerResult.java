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
package org.apache.beam.runners.jstorm;

import java.io.IOException;
import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.transforms.Aggregator;
import org.joda.time.Duration;

/**
 * A {@link PipelineResult} of executing {@link org.apache.beam.sdk.Pipeline Pipelines} using
 * {@link JStormRunner}.
 */
public class JStormRunnerResult implements PipelineResult {
    @Override
    public State getState() {
        throw new UnsupportedOperationException("This method is not yet supported.");
    }

    @Override
    public State cancel() throws IOException {
        throw new UnsupportedOperationException("This method is not yet supported.");
    }

    @Override
    public State waitUntilFinish(Duration duration) {
        throw new UnsupportedOperationException("This method is not yet supported.");
    }

    @Override
    public State waitUntilFinish() {
        throw new UnsupportedOperationException("This method is not yet supported.");
    }

    @Override
    public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator)
            throws AggregatorRetrievalException {
        throw new UnsupportedOperationException("This method is not yet supported.");
    }

    @Override
    public MetricResults metrics() {
        throw new UnsupportedOperationException("This method is not yet supported.");
    }
}
