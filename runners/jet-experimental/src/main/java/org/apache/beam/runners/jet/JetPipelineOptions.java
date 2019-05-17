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
package org.apache.beam.runners.jet;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/** Pipeline options specific to the Jet runner. */
public interface JetPipelineOptions extends PipelineOptions {

  @Description("Name of Jet group")
  @Validation.Required
  String getJetGroupName();

  void setJetGroupName(String jetGroupName);

  @Description("Specifies the addresses of the Jet cluster; needed only with external clusters")
  @Validation.Required
  String getJetClusterAddresses();

  void setJetClusterAddresses(String jetClusterAddresses);

  @Description(
      "Specifies where the fat-jar containing all the code is located; needed only with external clusters")
  @Validation.Required
  String getCodeJarLocation();

  void setCodeJarLocation(String codeJarLocation);

  @Description("Local parallelism of Jet nodes")
  @Validation.Required
  @Default.Integer(-1)
  Integer getJetLocalParallelism();

  void setJetLocalParallelism(Integer localParallelism);

  @Description("Specifies if the Runner should start its own Jet cluster")
  @Validation.Required
  @Default.Boolean(true)
  Boolean getJetStartOwnCluster();

  void setJetStartOwnCluster(Boolean startOwnCluser);

  @Description("Specifies how many cluster members should be started, if any")
  @Validation.Required
  @Default.Integer(2)
  Integer getJetClusterMemberCount();

  void setJetClusterMemberCount(Integer clusterMemberCount);
}
