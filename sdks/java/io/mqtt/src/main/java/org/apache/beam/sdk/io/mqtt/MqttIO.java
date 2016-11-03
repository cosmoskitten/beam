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
package org.apache.beam.sdk.io.mqtt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unbounded source for MQTT broker.
 *
 * <h3>Reading from a MQTT broker</h3>
 *
 * <p>MqttIO source returns an unbounded {@link PCollection} containing MQTT message
 * payloads (as {@code byte[]}).
 *
 * <p>To configure a MQTT source, you have to provide a MQTT connection configuration including
 * {@code ClientId}, a {@code ServerURI}, and a {@code Topic} pattern. The following
 * example illustrates various options for configuring the source:
 *
 * <pre>{@code
 *
 * pipeline.apply(
 *   MqttIO.read()
 *    .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create(
 *      "tcp://host:11883",
 *      "my_client_id",
 *      "my_topic"))
 *
 * }</pre>
 *
 * <h3>Writing to a MQTT broker</h3>
 *
 * <p>MqttIO sink supports writing {@code byte[]} to a topic on a MQTT broker.
 *
 * <p>To configure a MQTT sink, as for the read, you have to specify a MQTT connection
 * configuration with {@code ClientId}, {@code ServerURI}, {@code Topic}.
 *
 * <p>Optionally, you can also specify the {@code Retained} and {@code QoS} of the MQTT
 * message.
 *
 * <p>For instance:
 *
 * <pre>{@code
 *
 * pipeline
 *   .apply(...) // provide PCollection<byte[]>
 *   .MqttIO.write()
 *     .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create(
 *       "tcp://host:11883",
 *       "my_client_id",
 *       "my_topic"))
 *
 * }</pre>
 */
public class MqttIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(MqttIO.class);

  public static Read read() {
    return new AutoValue_MqttIO_Read.Builder()
        .setMaxReadTime(null).setMaxNumRecords(Long.MAX_VALUE).build();
  }

  public static Write write() {
    return new AutoValue_MqttIO_Write.Builder()
        .setRetained(false)
        .setQos(0)
        .build();
  }

  private MqttIO() {
  }

  /**
   * A POJO describing a MQTT connection.
   */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    @Nullable abstract String serverUri();
    @Nullable abstract String clientId();
    @Nullable abstract String topic();

    public static ConnectionConfiguration create(String serverUri, String clientId,
                                                 String topic) {
      checkNotNull(serverUri, "serverUri");
      checkNotNull(clientId, "clientId");
      checkNotNull(topic, "topic");
      return new AutoValue_MqttIO_ConnectionConfiguration(serverUri, clientId, topic);
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("serverUri", serverUri()));
      builder.add(DisplayData.item("clientId", clientId()));
      builder.add(DisplayData.item("topic", topic()));
    }

    private MqttClient getClient() throws MqttException {
      MqttClient client = new MqttClient(serverUri(), clientId());
      client.connect();
      return client;
    }

  }

  /**
   * A {@link PTransform} to read from a MQTT broker.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<byte[]>> {

    @Nullable abstract ConnectionConfiguration connectionConfiguration();
    abstract long maxNumRecords();
    @Nullable abstract Duration maxReadTime();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration config);
      abstract Builder setMaxNumRecords(long maxNumRecords);
      abstract Builder setMaxReadTime(Duration maxReadTime);
      abstract Read build();
    }

    /**
     * Define the MQTT connection configuration used to connect to the MQTT broker.
     */
    public Read withConnectionConfiguration(ConnectionConfiguration configuration) {
      checkNotNull(configuration, "ConnectionConfiguration");
      return toBuilder().setConnectionConfiguration(configuration).build();
    }

    /**
     * Define the max number of records received by the {@link Read}.
     * When this max number of records is lower than {@code Long.MAX_VALUE}, the {@link Read}
     * will provide a bounded {@link PCollection}.
     */
    public Read withMaxNumRecords(long maxNumRecords) {
      checkArgument(maxReadTime() == null,
          "maxNumRecord and maxReadTime are exclusive");
      return toBuilder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time (duration) while the {@link Read} will receive messages.
     * When this max read time is not null, the {@link Read} will provide a bounded
     * {@link PCollection}.
     */
    public Read withMaxReadTime(Duration maxReadTime) {
      checkArgument(maxNumRecords() == Long.MAX_VALUE,
          "maxNumRecord and maxReadTime are exclusive");
      return toBuilder().setMaxReadTime(maxReadTime).build();
    }

    @Override
    public PCollection<byte[]> apply(PBegin input) {

      org.apache.beam.sdk.io.Read.Unbounded<byte[]> unbounded =
          org.apache.beam.sdk.io.Read.from(new UnboundedMqttSource(this));

      PTransform<PBegin, PCollection<byte[]>> transform = unbounded;

      if (maxNumRecords() != Long.MAX_VALUE) {
        transform = unbounded.withMaxNumRecords(maxNumRecords());
      } else if (maxReadTime() != null) {
        transform = unbounded.withMaxReadTime(maxReadTime());
      }

      return input.getPipeline().apply(transform);
    }

    @Override
    public void validate(PBegin input) {
      // validation is performed in the ConnectionConfiguration create()
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      connectionConfiguration().populateDisplayData(builder);
      if (maxNumRecords() != Long.MAX_VALUE) {
        builder.add(DisplayData.item("maxNumRecords", maxNumRecords()));
      }
      builder.addIfNotNull(DisplayData.item("maxReadTime", maxReadTime()));
    }

  }

  private static class UnboundedMqttSource
      extends UnboundedSource<byte[], UnboundedSource.CheckpointMark> {

    private final Read spec;

    public UnboundedMqttSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public UnboundedReader<byte[]> createReader(PipelineOptions options,
                                        CheckpointMark checkpointMark) {
      return new UnboundedMqttReader(this);
    }

    @Override
    public List<UnboundedMqttSource> generateInitialSplits(int desiredNumSplits,
                                                           PipelineOptions options) {
      List<UnboundedMqttSource> sources = new ArrayList<>();
      for (int i = 0; i < desiredNumSplits; i++) {
        sources.add(new UnboundedMqttSource(spec));
      }
      return sources;
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
    }

    @Override
    public Coder<UnboundedSource.CheckpointMark> getCheckpointMarkCoder() {
      return new CheckpointCoder();
    }

    @Override
    public Coder<byte[]> getDefaultOutputCoder() {
      return ByteArrayCoder.of();
    }
  }

  /**
   * Checkpoint coder acting as a {@link VoidCoder}.
   */
  private static class CheckpointCoder extends AtomicCoder<UnboundedSource.CheckpointMark> {

    @Override
    public void encode(UnboundedSource.CheckpointMark value, OutputStream outStream, Context
        context) throws CoderException, IOException {
      // nothing to write
    }

    @Override
    public UnboundedSource.CheckpointMark decode(InputStream inStream, Context context) throws
        CoderException, IOException {
      // nothing to read
      return null;
    }
  }

  /**
   * POJO used to store MQTT message and its timestamp.
   */
  private static class MqttMessageWithTimestamp {

    private final MqttMessage message;
    private final Instant timestamp;

    public MqttMessageWithTimestamp(MqttMessage message, Instant timestamp) {
      this.message = message;
      this.timestamp = timestamp;
    }
  }

  private static class UnboundedMqttReader extends UnboundedSource.UnboundedReader<byte[]> {

    private final UnboundedMqttSource source;

    private MqttClient client;
    private byte[] current;
    private Instant currentTimestamp;
    private BlockingQueue<MqttMessageWithTimestamp> queue;

    private UnboundedMqttReader(UnboundedMqttSource source) {
      this.source = source;
      this.current = null;
      this.queue = new LinkedBlockingQueue<>();
    }

    @Override
    public boolean start() throws IOException {
      LOGGER.debug("Starting MQTT reader");
      Read spec = source.spec;
      try {
        client = spec.connectionConfiguration().getClient();
        client.subscribe(spec.connectionConfiguration().topic());
        client.setCallback(new MqttCallback() {
          @Override
          public void connectionLost(Throwable cause) {
            LOGGER.warn("MQTT connection lost", cause);
          }

          @Override
          public void messageArrived(String topic, MqttMessage message) throws Exception {
            LOGGER.trace("Message arrived");
            queue.put(new MqttMessageWithTimestamp(message, Instant.now()));
          }

          @Override
          public void deliveryComplete(IMqttDeliveryToken token) {
            // nothing to do
          }
        });
        return advance();
      } catch (MqttException e) {
        throw new IOException(e);
      }
    }

    @Override
    public boolean advance() throws IOException {
      LOGGER.debug("Taking from the pending queue ({})", queue.size());
      MqttMessageWithTimestamp message = null;
      try {
        message = queue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      if (message == null) {
        current = null;
        currentTimestamp = null;
        return false;
      }
      current = message.message.getPayload();
      currentTimestamp = message.timestamp;
      return true;
    }

    @Override
    public void close() throws IOException {
      LOGGER.debug("Closing MQTT reader");
      try {
        if (client != null) {
          try {
            client.disconnect();
          } finally {
            client.close();
          }
        }
      } catch (MqttException mqttException) {
        throw new IOException(mqttException);
      }
    }

    @Override
    public Instant getWatermark() {
      MqttMessageWithTimestamp message = queue.peek();
      if (message == null) {
        // no message yet in the queue, returning the min possible timestamp value
        return BoundedWindow.TIMESTAMP_MIN_VALUE;
      } else {
        // watermark is the timestamp of the oldest message in the queue
        return message.timestamp;
      }
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
      return new UnboundedSource.CheckpointMark() {
        @Override
        public void finalizeCheckpoint() throws IOException {
          // nothing to do
        }
      };
    }

    @Override
    public byte[] getCurrent() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public Instant getCurrentTimestamp() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return currentTimestamp;
    }

    @Override
    public UnboundedMqttSource getCurrentSource() {
      return source;
    }

  }

  /**
   * A {@link PTransform} to write and send a message to a MQTT server.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<byte[]>, PDone> {

    @Nullable abstract ConnectionConfiguration connectionConfiguration();
    @Nullable abstract int qos();
    @Nullable abstract boolean retained();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration configuration);
      abstract Builder setQos(int qos);
      abstract Builder setRetained(boolean retained);
      abstract Write build();
    }

    /**
     * Define MQTT connection configuration used to connect to the MQTT broker.
     */
    public Write withConnectionConfiguration(ConnectionConfiguration configuration) {
      return toBuilder().setConnectionConfiguration(configuration).build();
    }

    /**
     * Define the MQTT message quality of service.
     *
     * <ul>
     * <li>Quality of Service 0 - indicates that a message should
     * be delivered at most once (zero or one times). The message will not be persisted to disk,
     * and will not be acknowledged across the network. This QoS is the fastest,
     * but should only be used for messages which are not valuable - note that
     * if the server cannot process the message (for example, there
     * is an authorization problem), then the message will be silently dropped.
     * Also known as "fire and forget".</li>
     *
     * <li>Quality of Service 1 - indicates that a message should
     * be delivered at least once (one or more times).  The message can only be delivered safely if
     * it can be persisted on the broker.
     * If the broker can't persist the message, the message will not be
     * delivered in the event of a subscriber failure.
     * The {@link MqttIO.Write} with this QoS guarantee that all messages written to it will be
     * delivered to subscribers at least once.
     * This is the default QoS.</li>
     *
     * <li>Quality of Service 2 - indicates that a message should
     * be delivered once. The message will be persisted to disk on the broker, and will
     * be subject to a two-phase acknowledgement across the network.
     * The message can only be delivered safely if
     * it can be persisted on the broker.
     * If a persistence mechanism is not specified on the broker, the message will not be
     * delivered in the event of a client failure.
     * This QoS is not supported by {@link MqttIO}.</li>
     * </ul>
     *
     * <p>If persistence is not configured, QoS 1 and 2 messages will still be delivered
     * in the event of a network or server problem as the client will hold state in memory.
     * If the MQTT client is shutdown or fails and persistence is not configured then
     * delivery of QoS 1 and 2 messages can not be maintained as client-side state will
     * be lost.
     *
     * <p>For now, MqttIO fully supports QoS 0 and 1 (delivery at least once). QoS 2 is for now
     * limited and use with care, as it can result to duplication of message (delivery exactly
     * once).
     *
     * @param qos The quality of service value.
     * @return The {@link Write} {@link PTransform} with the corresponding QoS configuration.
     */
    public Write withQoS(int qos) {
      return toBuilder().setQos(qos).build();
    }

    /**
     * Whether or not the publish message should be retained by the messaging engine.
     * Sending a message with the retained set to {@code false} will clear the
     * retained message from the server. The default value is {@code false}.
     *
     * @param retained Whether or not the messaging engine should retain the message.
     * @return The {@link Write} {@link PTransform} with the corresponding retained configuration.
     */
    public Write withRetained(boolean retained) {
      return toBuilder().setRetained(retained).build();
    }

    @Override
    public PDone apply(PCollection<byte[]> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<byte[]> input) {
      checkArgument(qos() != 0 || qos() != 1, "Supported QoS are 0 and 1");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      connectionConfiguration().populateDisplayData(builder);
      builder.add(DisplayData.item("qos", qos()));
      builder.add(DisplayData.item("retained", retained()));
    }

    private static class WriteFn extends DoFn<byte[], Void> {

      private final Write spec;

      private transient MqttClient client;

      public WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void createMqttClient() throws Exception {
        client = spec.connectionConfiguration().getClient();
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        byte[] payload = context.element();
        MqttMessage message = new MqttMessage();
        message.setQos(spec.qos());
        message.setRetained(spec.retained());
        message.setPayload(payload);
        client.publish(spec.connectionConfiguration().topic(), message);
      }

      @Teardown
      public void closeMqttClient() throws Exception {
        if (client != null) {
          try {
            client.disconnect();
          } finally {
            client.close();
          }
        }
      }

    }

  }

}
