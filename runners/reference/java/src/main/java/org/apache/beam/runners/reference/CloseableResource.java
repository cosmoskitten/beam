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
package org.apache.beam.runners.reference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * An {@link AutoCloseable} that wraps a resource that needs to be cleaned up but does not implement
 * {@link AutoCloseable} itself. Recipients of a {@link CloseableResource} are in general
 * responsible for cleanup. Not thread-safe.
 */
public class CloseableResource<T> implements AutoCloseable {

  private final T resource;
  private final Closer<T> closer;

  private boolean isClosed = false;

  private CloseableResource(T resource, Closer<T> closer) {
    this.resource = resource;
    this.closer = closer;
  }

  /** Creates a {@link CloseableResource} with the given resource and closer. */
  public static <T> CloseableResource<T> of(T resource, Closer<T> closer) {
    checkArgument(resource != null, "Resource must be non-null");
    checkArgument(closer != null, "%s must be non-null", Closer.class.getName());
    return new CloseableResource<>(resource, closer);
  }

  /** Gets the underlying resource. */
  public T get() {
    checkState(!isClosed, "% is closed", CloseableResource.class.getName());
    return resource;
  }

  /**
   * Closes the underlying resource. The closer will only be executed on the first call.
   *
   * @throws CloseException wrapping any exceptions thrown while closing
   */
  @Override
  public void close() throws CloseException {
    if (!isClosed) {
      try {
        closer.close(resource);
        isClosed = true;
      } catch (Exception e) {
        // Mark resource as closed even if we catch an exception.
        isClosed = true;
        throw new CloseException(e);
      }
    }
  }

  /** A function that knows how to clean up after a resource. */
  @FunctionalInterface
  public interface Closer<T> {
    void close(T resource) throws Exception;
  }

  /** An exception that wraps errors thrown while a resource is being closed. */
  public static class CloseException extends Exception {
    private CloseException(Exception e) {
      super("Error closing resource", e);
    }
  }
}
