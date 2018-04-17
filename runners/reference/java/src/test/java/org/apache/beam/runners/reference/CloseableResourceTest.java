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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.reference.CloseableResource.CloseException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CloseableResource} */
@RunWith(JUnit4.class)
public class CloseableResourceTest {

  @Test
  public void alwaysReturnsSameResource() {
    Foo foo = new Foo();
    CloseableResource<Foo> resource = CloseableResource.of(foo, (ignored) -> {});
    assertThat(resource.get(), is(foo));
    assertThat(resource.get(), is(foo));
  }

  @Test
  public void callsCloser() throws Exception {
    AtomicBoolean closed = new AtomicBoolean(false);
    try (CloseableResource<Foo> ignored =
        CloseableResource.of(
            new Foo(),
            (foo) -> {
              closed.set(true);
            })) {
      // Do nothing.
    }
    assertTrue(closed.get());
  }

  @Test
  public void wrapsExceptionsInCloseException() {
    Exception wrapped = new Exception();
    CloseException closeException = null;
    try (CloseableResource<Foo> ignored =
        CloseableResource.of(
            new Foo(),
            (foo) -> {
              throw wrapped;
            })) {
      // Do nothing.
    } catch (CloseException e) {
      closeException = e;
    }
    assertThat(closeException, is(notNullValue()));
    assertThat(closeException, is(instanceOf(CloseException.class)));
    assertThat(closeException.getCause(), is(wrapped));
  }

  private static class Foo {}
}
