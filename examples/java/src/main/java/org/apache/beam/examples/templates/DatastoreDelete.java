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

package org.apache.beam.examples.templates;

import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;


/**
 * A template that deletes entities from Cloud Datastore using DatastoreIO.
 *
 * <p>This example shows how to use DatastoreIO to delete entities from Cloud Datastore. Note that
 * this example will write data to Cloud Datastore, which may incur charge for Cloud Datastore
 * operations.
 *
 * <p>TODO - Add documentation
 *
 */
public class DatastoreDelete {
  /**
   * Options supported by {@link DatastoreDelete}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions {
    @Description("Project ID under which Cloud Datastore Query needs to run on.")
    ValueProvider<String> getDatastoreProject();
    void setDatastoreProject(ValueProvider<String> value);

    @Description("Cloud Datastore GQL Query whose entities need to be deleted.")
    ValueProvider<String> getGqlQuery();
    void setGqlQuery(ValueProvider<String> value);

    @Description("Cloud Datastore Namespace")
    ValueProvider<String> getNamespace();
    void setNamespace(@Nullable ValueProvider<String> value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory
        .fromArgs(args).withValidation().as(DatastoreDelete.Options.class);

    // A transform to read entities
    DatastoreV1.Read readEntities = DatastoreIO.v1().read()
        .withProjectId(options.getDatastoreProject())
        .withGqlQuery(options.getGqlQuery())
        .withNamespace(options.getNamespace());

    // A transform to delete entities.
    DatastoreV1.DeleteEntity deleteEntities = DatastoreIO.v1().deleteEntity()
        .withProjectId(options.getDatastoreProject());

    Pipeline p = Pipeline.create(options);
    p.apply("ReadEntities", readEntities)
        .apply("DeleteEntities", deleteEntities);

    p.run();
  }
}
