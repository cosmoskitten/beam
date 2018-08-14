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
package org.apache.beam.sdk.options;

import java.util.SortedSet;

/** An exception used when encounter unexpected properties. */
public class PipelineOptionUnexpectedPropertyException extends IllegalArgumentException {
  private static final long serialVersionUID = 3265630128856068164L;

  private String message;

  public PipelineOptionUnexpectedPropertyException(String var1) {
    super(var1);
  }

  public void createUnexpectedPropertyExceptionMessage(String propertyName) {
    message =
        String.format(
            "Pipeline option '%s' is not supported in this context. It can be "
                + "cleared by 'RESET %s'.",
            propertyName, propertyName);
  }

  public void createUnexpectedPropertyExceptionMessage(String propertyName, String bestMatch) {
    createUnexpectedPropertyExceptionMessage(propertyName);
    message += String.format(" Did you mean 'SET %s'?", bestMatch);
  }

  public void createUnexpectedPropertyExceptionMessage(
      String propertyName, SortedSet<String> bestMatches) {
    createUnexpectedPropertyExceptionMessage(propertyName);
    message += String.format(" Did you mean SET one of %s?", bestMatches);
  }

  public String getUnexpectedPropertyExceptionMessage() {
    return message;
  }
}
