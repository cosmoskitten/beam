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

package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableSchema;
import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * FILL In this javadoc.
 */
public abstract class DynamicDestinations<T, DestinationT> implements Serializable {
  private PCollectionView<Map<String, String>> sideInput;
  private Map<String, String> materialized;

  public DynamicDestinations withSideInput(PCollectionView<Map<String, String>> sideInput) {
    this.sideInput = sideInput;
    return this;
  }

  /**
   * Returns an object that represents at a high level which table is being written to.
   */
  public abstract DestinationT getDestination(ValueInSingleWindow<T> element);

  /**
   * Returns the coder for {@link DestinationT}.
   */
  public @Nullable Coder<DestinationT> getDestinationCoder() {
    return null;
  }

  /**
   * Returns a {@link TableDestination} object for the destination.
   */
  public abstract TableDestination getTable(DestinationT destination);

  /**
   * Returns the table schema for the destination.
   */
  public abstract TableSchema getSchema(DestinationT destination);

  public PCollectionView<Map<String, String>> getSideInput() {
    return sideInput;
  }

  /**
   * Returns the materialized value of the side input. Can be called by concrete
   * {@link DynamicDestinations} instances in {@link #getSchema} or {@link #getTable}.
   */
  public Map<String, String> getSideInputValue() {
    return materialized;
  }

  void setSideInputValue(Map<String, String> value) {
    materialized = value;
  }
}
