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
package org.apache.beam.runners.dataflow.worker;

import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;

/**
 * A base class for {@link ParDoFn} implementations for overriding particular methods while
 * forwarding the rest to an existing instance.
 */
public abstract class ForwardingParDoFn implements ParDoFn {

  private ParDoFn delegate;

  protected ForwardingParDoFn(ParDoFn delegate) {
    this.delegate = delegate;
  }

  @Override
  public void startBundle(Receiver... receivers) throws Exception {
    delegate.startBundle(receivers);
  }

  @Override
  public void processElement(Object elem) throws Exception {
    delegate.processElement(elem);
  }

  @Override
  public void processTimers() throws Exception {
    delegate.processTimers();
  }

  @Override
  public void finishBundle() throws Exception {
    delegate.finishBundle();
  }

  @Override
  public void abort() throws Exception {
    delegate.abort();
  }
}
