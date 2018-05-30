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
package org.apache.beam.sdk.io.rabbitmq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.qpid.server.Broker;
import org.apache.qpid.server.BrokerOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test of {@link RabbitMqIO}.
 */
public class RabbitMqIOTest implements Serializable {

  public int port;

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  private transient Broker broker;

  @Before
  public void startBroker() throws Exception {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      port = serverSocket.getLocalPort();
    }

    broker = new Broker();
    BrokerOptions options = new BrokerOptions();
    options.setConfigProperty("qpid.amqp_port", String.valueOf(port));
    options.setConfigProperty("qpid.work_dir", temporaryFolder.newFolder().toString());
    options.setConfigProperty("qpid.home_dir", "src/test/qpid");
    broker.startup(options);
  }

  @After
  public void stopBroker() throws Exception {
    broker.shutdown();
  }

  @Test
  public void testReadQueue() throws Exception {
    PCollection<RabbitMqMessage> raw = pipeline.apply(
        RabbitMqIO.read().withUri("amqp://gues:guess@localhost:" + port).withQueue("READ")
          .withMaxNumRecords(10));
    PCollection<byte[]> output = raw.apply(ParDo.of(new ConverterFn()));

    PAssert.that(output)
        .containsInAnyOrder("Test 0".getBytes(), "Test 1".getBytes(), "Test 2".getBytes(),
            "Test 3".getBytes(), "Test 4".getBytes(), "Test 5".getBytes(), "Test 6".getBytes(),
            "Test 7".getBytes(), "Test 8".getBytes(), "Test 9".getBytes());

    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUri("amqp://guest:guest@localhost:" + port);
    Connection connection = connectionFactory.newConnection();
    Channel channel = connection.createChannel();
    channel.queueDeclare("READ", false, false, false, null);
    for (int i = 0; i < 10; i++) {
      channel.basicPublish("", "READ", null, ("Test " + i).getBytes());
    }

    pipeline.run();

    channel.close();
    connection.close();
  }

  @Test(timeout = 60 * 1000)
  public void testReadExchange() throws Exception {
    PCollection<RabbitMqMessage> raw = pipeline.apply(
        RabbitMqIO.read().withUri("amqp://guest:guest@localhost:" + port)
            .withExchange("READ", "fanout", "test")
            .withMaxNumRecords(10));
    PCollection<byte[]> output = raw.apply(ParDo.of(new ConverterFn()));
    PAssert.that(output).containsInAnyOrder(
        "Test 0".getBytes(),
        "Test 1".getBytes(),
        "Test 2".getBytes(),
        "Test 3".getBytes(),
        "Test 4".getBytes(),
        "Test 5".getBytes(),
        "Test 6".getBytes(),
        "Test 7".getBytes(),
        "Test 8".getBytes(),
        "Test 9".getBytes());

    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUri("amqp://guest:guest@localhost:" + port);
    Connection connection = connectionFactory.newConnection();
    final Channel channel = connection.createChannel();
    channel.exchangeDeclare("READ", "fanout");
    Thread publisher = new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(5000);
        } catch (Exception e) {
          e.printStackTrace();
        }
        for (int i = 0; i < 10; i++) {
          try {
            channel.basicPublish("READ", "test", null, ("Test " + i).getBytes());
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    };
    publisher.start();
    pipeline.run();
    publisher.join();

    channel.close();
    connection.close();
  }

  @Test
  public void testWriteExchange() throws Exception {
    List<RabbitMqMessage> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      RabbitMqMessage message = new RabbitMqMessage(("Test " + i).getBytes());
      data.add(message);
    }
    pipeline.apply(Create.of(data))
        .apply(RabbitMqIO.write()
            .withUri("amqp://guest:guest@localhost:" + port)
            .withExchange("WRITE", "fanout"));

    final List<String> received = new ArrayList<>();
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUri("amqp://guest:guest@localhost:" + port);
    Connection connection = connectionFactory.newConnection();
    Channel channel = connection.createChannel();
    channel.exchangeDeclare("WRITE", "fanout");
    String queueName = channel.queueDeclare().getQueue();
    channel.queueBind(queueName, "WRITE", "");
    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag,
          Envelope envelope,
          AMQP.BasicProperties properties,
          byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        received.add(message);
      }
    };
    channel.basicConsume(queueName, true, consumer);

    pipeline.run();

    while (received.size() < 1000) {
      Thread.sleep(500);
    }

    assertEquals(1000, received.size());
    for (int i = 0; i < 1000; i++) {
      assertTrue(received.contains("Test " + i));
    }

    channel.close();
    connection.close();
  }

  private class ConverterFn extends DoFn<RabbitMqMessage, byte[]> {

    public ConverterFn() {}

    @ProcessElement
    public void processElement(ProcessContext processContext) {
      RabbitMqMessage message = processContext.element();
      processContext.output(message.getBody());
    }

  }

}
