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
package org.apache.beam.runners.fnexecution.environment;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;

/**
 * Environment for external-based execution. The environment is responsible for stopping the
 * process.
 */
public class ExternalEnvironment implements RemoteEnvironment {

  private final String url;
  private final RunnerApi.Environment environment;
  private final String workerId;
  private final InstructionRequestHandler instructionHandler;
  private final Object lock = new Object();

  private boolean isClosed;

  public static RemoteEnvironment create(
      String url,
      RunnerApi.Environment environment,
      String workerId,
      InstructionRequestHandler instructionHandler) {
    return new ExternalEnvironment(url, environment, workerId, instructionHandler);
  }

  private ExternalEnvironment(
      String url,
      RunnerApi.Environment environment,
      String workerId,
      InstructionRequestHandler instructionHandler) {

    this.url = url;
    this.environment = environment;
    this.workerId = workerId;
    this.instructionHandler = instructionHandler;
  }

  public static void stop(String url, String workerId) throws IOException {
    int responseCode =
        ((HttpURLConnection)
                new URL(String.format("%s/stop?%s", url, escapeParameter("workerId", workerId)))
                    .openConnection())
            .getResponseCode();
    if (responseCode != 200) {
      throw new RuntimeException(
          String.format(
              "Unable to start workers %s at %s (response code %s)", workerId, url, responseCode));
    }
  }

  public static void start(
      String url, String workerId, Map<String, String> args, Map<String, String> params)
      throws IOException {
    Preconditions.checkArgument(params.isEmpty());
    StringBuilder fullUrl = new StringBuilder(url);
    fullUrl.append("start?");
    fullUrl.append(escapeParameter("workerId", workerId));
    for (Map.Entry<String, String> arg : args.entrySet()) {
      fullUrl.append('&');
      fullUrl.append(escapeParameter(arg.getKey(), arg.getValue()));
    }
    int responseCode =
        ((HttpURLConnection) new URL(fullUrl.toString()).openConnection()).getResponseCode();
    if (responseCode != 200) {
      throw new RuntimeException(
          String.format(
              "Unable to stop workers %s at %s (response code %s)", workerId, url, responseCode));
    }
  }

  private static String escapeParameter(String name, String value)
      throws UnsupportedEncodingException {
    return String.format(
        "%s=%s", URLEncoder.encode(name, "UTF-8"), URLEncoder.encode(value, "UTF-8"));
  }

  @Override
  public RunnerApi.Environment getEnvironment() {
    return environment;
  }

  @Override
  public InstructionRequestHandler getInstructionRequestHandler() {
    return instructionHandler;
  }

  @Override
  public void close() throws Exception {
    synchronized (lock) {
      if (!isClosed) {
        stop(url, workerId);
        instructionHandler.close();
        isClosed = true;
      }
    }
  }
}
