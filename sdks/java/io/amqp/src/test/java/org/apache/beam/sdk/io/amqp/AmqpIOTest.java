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
package org.apache.beam.sdk.io.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.messenger.Messenger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests on {@link AmqpIO}.
 */
public class AmqpIOTest {

  private static final Logger LOG = LoggerFactory.getLogger(AmqpIOTest.class);

  private static int port;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Before
  public void findFreeNetworkPort() throws Exception {
    LOG.info("Finding free network port");
    ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
    socket.close();
  }

  @Test
  public void testRead() throws Exception {
    PCollection<String> output = pipeline.apply(AmqpIO.read()
        .withMaxNumRecords(10)
        .withAddresses(Collections.singletonList("amqp://~localhost:" + port)));
    PAssert.thatSingleton(output.apply(Count.<String>globally())).isEqualTo(10L);
    PAssert.that(output).containsInAnyOrder(
        "AmqpValue{Test 0}",
        "AmqpValue{Test 1}",
        "AmqpValue{Test 2}",
        "AmqpValue{Test 3}",
        "AmqpValue{Test 4}",
        "AmqpValue{Test 5}",
        "AmqpValue{Test 6}",
        "AmqpValue{Test 7}",
        "AmqpValue{Test 8}",
        "AmqpValue{Test 9}"
    );

    Thread sender = new Thread() {
      public void run() {
        try {
          Thread.sleep(500);
          Messenger sender = Messenger.Factory.create();
          sender.start();
          for (int i = 0; i < 10; i++) {
            Message message = Message.Factory.create();
            message.setAddress("amqp://localhost:" + port);
            message.setBody(new AmqpValue("Test " + i));
            sender.put(message);
            sender.send();
          }
          sender.stop();
        } catch (Exception e) {
          LOG.error("Sender error", e);
        }
      }
    };
    sender.start();
    pipeline.run();
    sender.join();
  }

  @Test
  public void testWrite() throws Exception {
    final List<String> received = new ArrayList<>();
    Thread receiver = new Thread() {
      @Override
      public void run() {
        try {
          Messenger messenger = Messenger.Factory.create();
          messenger.start();
          messenger.subscribe("amqp://~localhost:" + port);
          while (received.size() < 1000) {
            messenger.recv();
            while (messenger.incoming() > 0) {
              Message message = messenger.get();
              LOG.info("Received: " + message.getBody().toString());
              received.add(message.getBody().toString());
            }
          }
          messenger.stop();
        } catch (Exception e) {
          LOG.error("Receiver error", e);
        }
      }
    };
    LOG.info("Starting AMQP receiver");
    receiver.start();

    List<String> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      data.add("Test " + i);
    }
    pipeline.apply(Create.of(data))
        .apply(AmqpIO.write()
            .withAddresses(Collections.singletonList("amqp://localhost:" + port)));
    LOG.info("Starting pipeline");
    pipeline.run();
    LOG.info("Join receiver thread");
    receiver.join();

    assertEquals(1000, received.size());
    for (int i = 0; i < 1000; i++) {
      assertTrue(received.contains("AmqpValue{Test " + i + "}"));
    }
  }

}
