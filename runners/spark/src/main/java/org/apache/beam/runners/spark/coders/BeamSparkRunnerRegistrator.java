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

package org.apache.beam.runners.spark.coders;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.beam.runners.spark.io.MicrobatchSource;
import org.apache.beam.runners.spark.io.SourceRDD;
import org.apache.spark.serializer.KryoRegistrator;


/**
 * Custom {@link KryoRegistrator}s for Beam's Spark runner needs.
 */
public class BeamSparkRunnerRegistrator implements KryoRegistrator {

  /**
   * Register coders and sources with {@link JavaSerializer} since they aren't guaranteed to be
   * Kryo-serializable.
   */
  @Override
  public void registerClasses(Kryo kryo) {
    kryo.register(MicrobatchSource.class, new JavaSerializer());
    kryo.register(SourceRDD.Bounded.BoundedSourceWrapper.class, new JavaSerializer());
    kryo.register(CoderWrapper.class, new JavaSerializer());
  }
}
