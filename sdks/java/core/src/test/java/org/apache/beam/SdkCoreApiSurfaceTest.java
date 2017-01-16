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
package org.apache.beam;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Set;
import org.apache.beam.sdk.util.ApiSurface;
import org.apache.beam.sdk.util.ApiSurfaceVerificationTest;
import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * API surface verification for {@link org.apache.beam}.
 */
@RunWith(JUnit4.class)
public class SdkCoreApiSurfaceTest extends ApiSurfaceVerificationTest {

  @Override
  protected ApiSurface apiSurface() throws IOException {
    return ApiSurface.getSdkApiSurface();
  }

  @Override
  protected Set<Matcher<Class<?>>> allowedPackages() {
    return
        ImmutableSet.of(
            inPackage("org.apache.beam"),
            inPackage("com.google.api.client"),
            inPackage("com.google.api.services.bigquery"),
            inPackage("com.google.api.services.cloudresourcemanager"),
            inPackage("com.google.api.services.pubsub"),
            inPackage("com.google.api.services.storage"),
            inPackage("com.google.auth"),
            inPackage("com.google.protobuf"),
            inPackage("com.fasterxml.jackson.annotation"),
            inPackage("com.fasterxml.jackson.core"),
            inPackage("com.fasterxml.jackson.databind"),
            inPackage("org.apache.avro"),
            inPackage("org.hamcrest"), // via DataflowMatchers
            inPackage("org.codehaus.jackson"), // via Avro
            inPackage("org.joda.time"));
  }
}
