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

package org.apache.beam.runners.gearpump.translators.io;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.gearpump.Message;
import org.apache.gearpump.streaming.source.DataSource;
import org.apache.gearpump.streaming.task.TaskContext;
import org.joda.time.Instant;

import java.io.IOException;

/**
 * wrapper over UnboundedSource for Gearpump DataSource API.
 */
public class GearpumpSource<OutputT, CheckpointMarkT extends UnboundedSource.CheckpointMark>
    implements DataSource {

  private final byte[] serializedOptions;

  private final UnboundedSource<OutputT, CheckpointMarkT> source;
  private UnboundedSource.UnboundedReader<OutputT> reader;
  private boolean available = false;

  public GearpumpSource(UnboundedSource<OutputT, CheckpointMarkT> source, PipelineOptions options) {
    this.source = source;
    try {
      this.serializedOptions = new ObjectMapper().writeValueAsBytes(options);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void open(TaskContext context, long startTime) {
    try {
      PipelineOptions options = new ObjectMapper()
          .readValue(serializedOptions, PipelineOptions.class);
      reader = source.createReader(options, null);
      available = reader.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      close();
    }
  }

  @Override
  public Message read() {
    Message message = null;
    try {
      if (available) {
        OutputT data = reader.getCurrent();
        Instant timestamp = reader.getCurrentTimestamp();
        available = reader.advance();
        message = Message.apply(
            WindowedValue.of(data, timestamp, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING),
            timestamp.getMillis());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      close();
    }
    return message;
  }

  @Override
  public void close() {
    try {
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
