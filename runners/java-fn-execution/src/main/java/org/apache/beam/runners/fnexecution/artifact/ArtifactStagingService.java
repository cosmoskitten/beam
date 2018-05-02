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

package org.apache.beam.runners.fnexecution.artifact;

import org.apache.beam.runners.fnexecution.FnService;

/** An implementation of the Beam Artifact Staging Service. */
public interface ArtifactStagingService extends FnService {
  /**
   * Get an artifact source that can access staged artifacts.
   *
   * <p>Once an artifact staging service has staged artifacts, runners need a way to access them.
   * Thus this method provides an ArtifactSource that can access them.
   */
  ArtifactSource createAccessor();
}
