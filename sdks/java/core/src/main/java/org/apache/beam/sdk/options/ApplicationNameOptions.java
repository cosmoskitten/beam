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

import com.google.common.base.MoreObjects;

import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Options that allow setting the application name.
 */
public interface ApplicationNameOptions extends PipelineOptions {
  /**
   * Name of application, for display purposes.
   *
   * <p>Defaults to the name of the class that constructs the {@link PipelineOptions}
   * via the {@link PipelineOptionsFactory}.
   */
  @Description("Name of application for display purposes. Defaults to the name of the class that "
      + "constructs the PipelineOptions via the PipelineOptionsFactory.")
  String getAppName();
  void setAppName(String value);

  @Description("A normalized unique name that is used to name anything related to the pipeline."
      + "It defaults to ApplicationName-UserName-Date-RandomInteger")
  @Default.InstanceFactory(NormalizedUniqueNameFactory.class)
  String getNormalizedUniqueName();
  void setNormalizedUniqueName(String numWorkers);

  /**
   * Returns a normalized unique name constructed from {@link ApplicationNameOptions#getAppName()},
   * the local system user name (if available), the current time, and a random integer.
   *
   * <p>The normalization makes sure that the name matches the pattern of
   * [a-z]([-a-z0-9]*[a-z0-9])?.
   */
  public static class NormalizedUniqueNameFactory implements DefaultValueFactory<String> {
    private static final DateTimeFormatter FORMATTER =
        DateTimeFormat.forPattern("MMddHHmmss").withZone(DateTimeZone.UTC);

    @Override
    public String create(PipelineOptions options) {
      String appName = options.as(ApplicationNameOptions.class).getAppName();
      String normalizedAppName = appName == null || appName.length() == 0 ? "BeamApp"
          : appName.toLowerCase()
                   .replaceAll("[^a-z0-9]", "0")
                   .replaceAll("^[^a-z]", "a");
      String userName = MoreObjects.firstNonNull(System.getProperty("user.name"), "");
      String normalizedUserName = userName.toLowerCase()
                                          .replaceAll("[^a-z0-9]", "0");
      String datePart = FORMATTER.print(DateTimeUtils.currentTimeMillis());

      String randomPart = Integer.toHexString(ThreadLocalRandom.current().nextInt());
      return String.format("%s-%s-%s-%s",
          normalizedAppName, normalizedUserName, datePart, randomPart);
    }
  }
}
