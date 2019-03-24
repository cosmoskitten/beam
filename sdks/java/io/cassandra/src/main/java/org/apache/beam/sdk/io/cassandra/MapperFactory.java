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
package org.apache.beam.sdk.io.cassandra;

import com.datastax.driver.core.Session;

/**
 * This interface can be used to supply a custom Object Mapper to the Beam CassandraIO. To supply
 * CassandraIO with a custom mapper you need to implement a factory as well as a Mapper and pass it
 * to CassandraIO with the withCustomMapperFactory method as seen below:
 *
 * <pre>{@code
 * MapperFactory<CustomClass> factory = new MyCustomFactory();
 *
 * pipeline.apply(CassandraIO.<CustomClass>read()
 *     .withHosts(Arrays.asList("host1", "host2"))
 *     .withPort(9042)
 *     .withKeyspace("beam")
 *     .withTable("CustomClass")
 *     .withEntity(CustomClass.class)
 *     .withCustomMapperFactory(factory)
 *     .withCoder(SerializableCoder.of(CustomClass.class))
 * }</pre>
 *
 * @see org.apache.beam.sdk.io.cassandra.CassandraIO
 * @see org.apache.beam.sdk.io.cassandra.MapperFactory
 * @see org.apache.beam.sdk.io.cassandra.Mapper
 */
public interface MapperFactory<T> {

  Mapper<T> getMapper(Session session, Class<T> entity);
}
