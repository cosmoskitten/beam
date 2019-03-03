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
package org.apache.beam.runners.core.construction.grpc;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.BindableService;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.Server;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;

/**
 * A {@link Server gRPC Server} which manages a single service. The lifetime of the service is bound
 * to the {@link GrpcServer}.
 */
public class GrpcServer<ServiceT extends BindableService & AutoCloseable> implements AutoCloseable {
  /** Create a {@link GrpcServer} for the provided service running on an arbitrary port. */
  public static <ServiceT extends BindableService & AutoCloseable>
      GrpcServer<ServiceT> allocatePortAndCreateFor(ServiceT service, ServerFactory factory)
          throws IOException {
    ApiServiceDescriptor.Builder apiServiceDescriptor = ApiServiceDescriptor.newBuilder();
    Server server =
        factory.allocateAddressAndCreate(ImmutableList.of(service), apiServiceDescriptor);
    return new GrpcServer<>(server, service, apiServiceDescriptor.build());
  }

  /**
   * Create a {@link GrpcServer} for the provided service which will run at the endpoint specified
   * in the {@link ApiServiceDescriptor}.
   */
  public static <ServiceT extends BindableService & AutoCloseable> GrpcServer<ServiceT> create(
      ServiceT service, ApiServiceDescriptor endpoint, ServerFactory factory) throws IOException {
    return new GrpcServer<>(factory.create(ImmutableList.of(service), endpoint), service, endpoint);
  }

  /** @deprecated This create function is used for Dataflow migration purpose only. */
  @Deprecated
  public static <ServiceT extends BindableService & AutoCloseable> GrpcServer<ServiceT> create(
      ServiceT service, ApiServiceDescriptor endpoint) {
    return new GrpcServer(null, service, endpoint) {
      @Override
      public void close() throws Exception {}
    };
  }

  private final Server server;
  private final ServiceT service;
  private final ApiServiceDescriptor apiServiceDescriptor;

  protected GrpcServer(Server server, ServiceT service, ApiServiceDescriptor apiServiceDescriptor) {
    this.server = server;
    this.service = service;
    this.apiServiceDescriptor = apiServiceDescriptor;
  }

  /**
   * Get an {@link ApiServiceDescriptor} describing the endpoint this {@link GrpcServer} is bound
   * to.
   */
  public ApiServiceDescriptor getApiServiceDescriptor() {
    return apiServiceDescriptor;
  }

  /** Get the service exposed by this {@link GrpcServer}. */
  public ServiceT getService() {
    return service;
  }

  /** Get the underlying {@link Server} contained by this {@link GrpcServer}. */
  public Server getServer() {
    return server;
  }

  @Override
  public void close() throws Exception {
    try {
      // The server has been closed, and should not respond to any new incoming calls.
      server.shutdown();
      service.close();
      server.awaitTermination(60, TimeUnit.SECONDS);
    } finally {
      server.shutdownNow();
      server.awaitTermination();
    }
  }
}
