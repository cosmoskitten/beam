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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.messenger.Messenger;
import org.apache.qpid.proton.messenger.Tracker;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * AmqpIO supports AMQP 1.0 protocol using the Apache QPid Proton-J library.
 *
 * <p>If you want to use AMQP 0.9 protocol, you might also be interested in the Apache Beam
 * RabbitMqIO.
 *
 * <p>It's also possible to use AMQP 1.0 protocol via Apache Qpid JMS connection factory and the
 * Apache Beam JmsIO.
 *
 * <h3>Binding AMQP and receive messages</h3>
 *
 * <p>The {@link AmqpIO} {@link Read} can bind a AMQP listener endpoint and receive messages. It can
 * also connect to a AMPQ broker (such as Apache Qpid or Apache ActiveMQ).
 *
 * <p>{@link AmqpIO} {@link Read} returns an unbounded {@link PCollection} of {@code String}
 * containing the received messages (as a {@link AmqpValue}).
 *
 * <p>To configure a AMQP source, you have to provide a list of addresses where it will receive
 * messages. An address has the following form: {@code
 * [amqp[s]://][user[:password]@]domain[/[name]]} where {@code domain} can be one of {@code
 * host | host:port | ip | ip:port | name}. NB: the {@code ~} character allows to bind a AMQP
 * listener instead of connecting to a remote broker. For instance {@code amqp://~0.0.0.0:1234}
 * will bind a AMQP listener on any network interface on the 1234 port number.
 *
 * <p>The following example illustrates how to configure a AMQP source:
 *
 * <pre>{@code
 *
 *  pipeline.apply(AmqpIO.read()
 *    .withAddresses(Collections.singletonList("amqp://host:1234")))
 *
 * }</pre>
 *
 * <h3>Sending messages to a AMQP endpoint</h3>
 *
 * <p>{@link AmqpIO} provides a sink to send {@link PCollection} elements as messages.
 *
 * <p>As for the {@link Read}, {@link AmqpIO} {@link Write} requires a list of addresses where to
 * send messages. The following example illustrates how to configure the {@link AmqpIO}
 * {@link Write}:
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(...) // provide PCollection<String>
 *    .apply(AmqpIO.write()
 *      .withAddresses(Collections.singletonList("amqp://host:1234")));
 *
 * }</pre>
 */
public class AmqpIO {

  public static Read read() {
    return new AutoValue_AmqpIO_Read.Builder().setMaxNumRecords(Long.MAX_VALUE).build();
  }

  public static Write write() {
    return new AutoValue_AmqpIO_Write.Builder().build();
  }

  private AmqpIO() {
  }

  /**
   * A {@link PTransform} to read/receive messages using AMQP 1.0 protocol.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    @Nullable abstract List<String> addresses();
    abstract long maxNumRecords();
    @Nullable abstract Duration maxReadTime();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setAddresses(List<String> addresses);
      abstract Builder setMaxNumRecords(long maxNumRecords);
      abstract Builder setMaxReadTime(Duration maxReadTime);
      abstract Read build();
    }

    /**
     * Define the AMQP addresses where to receive messages.
     */
    public Read withAddresses(List<String> addresses) {
      checkArgument(addresses != null, "AmqpIO.read().withAddresses(addresses) called with null"
          + " addresses");
      checkArgument(!addresses.isEmpty(), "AmqpIO.read().withAddresses(addresses) called with "
          + "empty addresses list");
      return builder().setAddresses(addresses).build();
    }

    /**
     * Define the max number of records received by the {@link Read}.
     * When the max number of records is lower than {@code Long.MAX_VALUE}, the {@link Read} will
     * provide a bounded {@link PCollection}.
     */
    public Read withMaxNumRecords(long maxNumRecords) {
      checkArgument(maxReadTime() == null,
          "maxNumRecord and maxReadTime are exclusive");
      return builder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time (duration) while the {@link Read} will receive messages.
     * When this max read time is not null, the {@link Read} will provide a bounded
     * {@link PCollection}.
     */
    public Read withMaxReadTime(Duration maxReadTime) {
      checkArgument(maxNumRecords() == Long.MAX_VALUE,
          "maxNumRecord and maxReadTime are exclusive");
      return builder().setMaxReadTime(maxReadTime).build();
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {
      checkState(addresses() != null, "AmqIO.read() requires addresses list to be set via "
          + "withAddresses(addresses)");
      checkState(!addresses().isEmpty(), "AmqIO.read() requires a non-empty addresses list to be"
          + " set via withAddresses(addresses)");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      StringBuilder stringBuilder = new StringBuilder();
      for (String address : addresses()) {
        stringBuilder.append(address).append(" ");
      }
      builder.add(DisplayData.item("addresses", stringBuilder.toString()));
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      org.apache.beam.sdk.io.Read.Unbounded<String> unbounded =
          org.apache.beam.sdk.io.Read.from(new UnboundedAmqpSource(this));

      PTransform<PBegin, PCollection<String>> transform = unbounded;

      if (maxNumRecords() != Long.MAX_VALUE) {
        transform = unbounded.withMaxNumRecords(maxNumRecords());
      } else if (maxReadTime() != null) {
        transform = unbounded.withMaxReadTime(maxReadTime());
      }

      return input.getPipeline().apply(transform);
    }

  }

  private static class AmqpCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

    private transient Messenger messenger;
    private transient List<Tracker> trackers = new ArrayList<>();
    private Instant watermark;

    public AmqpCheckpointMark() {
    }

    @Override
    public void finalizeCheckpoint() {
      for (Tracker tracker : trackers) {
        // flag as not cumulative
        messenger.accept(tracker, 0);
      }
      trackers.clear();
      watermark = Instant.now();
    }

    // set an empty list to messages when deserialize
    private void readObject(java.io.ObjectInputStream stream)
        throws java.io.IOException, ClassNotFoundException {
      trackers = new ArrayList<>();
      watermark = Instant.now();
    }

  }

  private static class UnboundedAmqpSource extends UnboundedSource<String, AmqpCheckpointMark> {

    private final Read spec;

    public UnboundedAmqpSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public List<UnboundedAmqpSource> split(int desiredNumSplits,
                                                           PipelineOptions pipelineOptions) {
      // amqp is a queue system, so, it's possible to have multiple concurrent sources, even if
      // they bind the listener
      List<UnboundedAmqpSource> sources = new ArrayList<>();
      if (desiredNumSplits > 0) {
        for (int i = 0; i < desiredNumSplits; i++) {
          sources.add(new UnboundedAmqpSource(spec));
        }
      } else {
        sources.add(new UnboundedAmqpSource(spec));
      }
      return sources;
    }

    @Override
    public UnboundedReader<String> createReader(PipelineOptions pipelineOptions,
                                                AmqpCheckpointMark checkpointMark) {
      return new UnboundedAmqpReader(this, checkpointMark);
    }

    @Override
    public Coder<String> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }

    @Override
    public Coder<AmqpCheckpointMark> getCheckpointMarkCoder() {
      return SerializableCoder.of(AmqpCheckpointMark.class);
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

  }

  private static class UnboundedAmqpReader extends UnboundedSource.UnboundedReader<String> {

    private final UnboundedAmqpSource source;

    private Messenger messenger;
    private String current;
    private Instant currentTimestamp;
    private AmqpCheckpointMark checkpointMark;

    public UnboundedAmqpReader(UnboundedAmqpSource source, AmqpCheckpointMark checkpointMark) {
      this.source = source;
      this.current = null;
      if (checkpointMark != null) {
        this.checkpointMark = checkpointMark;
      } else {
        this.checkpointMark = new AmqpCheckpointMark();
      }
    }

    @Override
    public Instant getWatermark() {
      return checkpointMark.watermark;
    }

    @Override
    public Instant getCurrentTimestamp() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return currentTimestamp;
    }

    @Override
    public String getCurrent() {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
      return checkpointMark;
    }

    @Override
    public UnboundedAmqpSource getCurrentSource() {
      return source;
    }

    @Override
    public boolean start() throws IOException {
      Read spec = source.spec;
      messenger = Messenger.Factory.create();
      messenger.start();
      for (String address : spec.addresses()) {
        messenger.subscribe(address);
      }
      checkpointMark.messenger = messenger;
      return advance();
    }

    @Override
    public boolean advance() {
      messenger.recv();
      if (messenger.incoming() > 0) {
        Message message = messenger.get();
        Tracker tracker = messenger.incomingTracker();
        checkpointMark.trackers.add(tracker);
        currentTimestamp = new Instant(message.getCreationTime());
        if (currentTimestamp.isBefore(checkpointMark.watermark)) {
          checkpointMark.watermark = currentTimestamp;
        }
        current = message.getBody().toString();
        return true;
      }
      current = null;
      return false;
    }

    @Override
    public void close() {
      if (messenger != null) {
        messenger.stop();
      }
    }

  }

  /**
   * A {@link PTransform} to send messages using AMQP 1.0 protocol.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, PDone> {

    @Nullable abstract List<String> addresses();
    @Nullable abstract String subject();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setAddresses(List<String> addresses);
      abstract Builder setSubject(String subject);
      abstract Write build();
    }

    /**
     * Define the AMQP addresses where to send messages.
     */
    public Write withAddresses(List<String> addresses) {
      checkArgument(addresses != null, "AmqpIO.write().withAddresses(addresses) called with "
          + "null addresses");
      checkArgument(!addresses.isEmpty(), "AmpqIO.write().withAddresses(addresses) called with "
          + "empty addresses list");
      return builder().setAddresses(addresses).build();
    }

    /**
     * Define the AMQP messages subject.
     */
    public Write withSubject(String subject) {
      checkArgument(subject != null, "AmqpIO.write().withSubject(subject) called with null "
          + "subject");
      return builder().setSubject(subject).build();
    }

    @Override
    public PDone expand(PCollection<String> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {
      checkState(addresses() != null, "AmqIO.write() requires addresses list to be set via "
          + "withAddresses(addresses)");
      checkState(!addresses().isEmpty(), "AmqIO.write() requires a non-empty addresses list to be"
          + " set via withAddresses(addresses)");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      StringBuilder stringBuilder = new StringBuilder();
      for (String address : addresses()) {
        stringBuilder.append(address).append(" ");
      }
      builder.add(DisplayData.item("addresses", stringBuilder.toString()));
      builder.addIfNotNull(DisplayData.item("subject", subject()));
    }

    private static class WriteFn extends DoFn<String, Void> {

      private final Write spec;

      private transient Messenger messenger;

      public WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        messenger = Messenger.Factory.create();
        messenger.start();
      }

      @ProcessElement
      public void processElement(ProcessContext processContext) throws Exception {
        String element = processContext.element();
        for (String address : spec.addresses()) {
          Message message = Message.Factory.create();
          message.setAddress(address);
          if (spec.subject() != null) {
            message.setSubject(spec.subject());
          }
          message.setBody(new AmqpValue(element));
          messenger.put(message);
          messenger.send();
        }
      }

      @Teardown
      public void teardown() throws Exception {
        if (messenger != null) {
          messenger.stop();
        }
      }

    }

  }

}
