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
package org.apache.beam.sdk.io;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PubsubOptions;
import org.apache.beam.sdk.runners.DirectPipelineRunner;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.PubsubClient;
import org.apache.beam.sdk.util.PubsubClient.IncomingMessage;
import org.apache.beam.sdk.util.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.util.PubsubClient.TransportType;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;

import com.google.common.base.Throwables;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Read and Write {@link PTransform}s for Cloud Pub/Sub streams. These transforms create
 * and consume unbounded {@link PCollection PCollections}.
 *
 * <h3>Permissions</h3>
 * <p>Permission requirements depend on the {@link PipelineRunner} that is used to execute the
 * Dataflow job. Please refer to the documentation of corresponding
 * {@link PipelineRunner PipelineRunners} for more details.
 */
public class PubsubIO {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubIO.class);

  /** The default {@link Coder} used to translate to/from Cloud Pub/Sub messages. */
  public static final Coder<String> DEFAULT_PUBSUB_CODER = StringUtf8Coder.of();

  /**
   * Project IDs must contain 6-63 lowercase letters, digits, or dashes.
   * IDs must start with a letter and may not end with a dash.
   * This regex isn't exact - this allows for patterns that would be rejected by
   * the service, but this is sufficient for basic parsing of table references.
   */
  private static final Pattern PROJECT_ID_REGEXP =
      Pattern.compile("[a-z][-a-z0-9:.]{4,61}[a-z0-9]");

  private static final Pattern SUBSCRIPTION_REGEXP =
      Pattern.compile("projects/([^/]+)/subscriptions/(.+)");

  private static final Pattern TOPIC_REGEXP = Pattern.compile("projects/([^/]+)/topics/(.+)");

  private static final Pattern V1BETA1_SUBSCRIPTION_REGEXP =
      Pattern.compile("/subscriptions/([^/]+)/(.+)");

  private static final Pattern V1BETA1_TOPIC_REGEXP = Pattern.compile("/topics/([^/]+)/(.+)");

  private static final Pattern PUBSUB_NAME_REGEXP = Pattern.compile("[a-zA-Z][-._~%+a-zA-Z0-9]+");

  private static final int PUBSUB_NAME_MIN_LENGTH = 3;
  private static final int PUBSUB_NAME_MAX_LENGTH = 255;

  private static final String SUBSCRIPTION_RANDOM_TEST_PREFIX = "_random/";
  private static final String SUBSCRIPTION_STARTING_SIGNAL = "_starting_signal/";
  private static final String TOPIC_DEV_NULL_TEST_NAME = "/topics/dev/null";

  private static void validateProjectName(String project) {
    Matcher match = PROJECT_ID_REGEXP.matcher(project);
    if (!match.matches()) {
      throw new IllegalArgumentException(
          "Illegal project name specified in Pubsub subscription: " + project);
    }
  }

  private static void validatePubsubName(String name) {
    if (name.length() < PUBSUB_NAME_MIN_LENGTH) {
      throw new IllegalArgumentException(
          "Pubsub object name is shorter than 3 characters: " + name);
    }
    if (name.length() > PUBSUB_NAME_MAX_LENGTH) {
      throw new IllegalArgumentException(
          "Pubsub object name is longer than 255 characters: " + name);
    }

    if (name.startsWith("goog")) {
      throw new IllegalArgumentException("Pubsub object name cannot start with goog: " + name);
    }

    Matcher match = PUBSUB_NAME_REGEXP.matcher(name);
    if (!match.matches()) {
      throw new IllegalArgumentException("Illegal Pubsub object name specified: " + name
          + " Please see Javadoc for naming rules.");
    }
  }

  /**
   * Class representing a Cloud Pub/Sub Subscription.
   */
  public static class PubsubSubscription implements Serializable {
    private enum Type { NORMAL, FAKE }

    private final Type type;
    private final String project;
    private final String subscription;

    private PubsubSubscription(Type type, String project, String subscription) {
      this.type = type;
      this.project = project;
      this.subscription = subscription;
    }

    /**
     * Creates a class representing a Pub/Sub subscription from the specified subscription path.
     *
     * <p>Cloud Pub/Sub subscription names should be of the form
     * {@code projects/<project>/subscriptions/<subscription>}, where {@code <project>} is the name
     * of the project the subscription belongs to. The {@code <subscription>} component must comply
     * with the following requirements:
     *
     * <ul>
     * <li>Can only contain lowercase letters, numbers, dashes ('-'), underscores ('_') and periods
     * ('.').</li>
     * <li>Must be between 3 and 255 characters.</li>
     * <li>Must begin with a letter.</li>
     * <li>Must end with a letter or a number.</li>
     * <li>Cannot begin with {@code 'goog'} prefix.</li>
     * </ul>
     */
    public static PubsubSubscription fromPath(String path) {
      if (path.startsWith(SUBSCRIPTION_RANDOM_TEST_PREFIX)
          || path.startsWith(SUBSCRIPTION_STARTING_SIGNAL)) {
        return new PubsubSubscription(Type.FAKE, "", path);
      }

      String projectName, subscriptionName;

      Matcher v1beta1Match = V1BETA1_SUBSCRIPTION_REGEXP.matcher(path);
      if (v1beta1Match.matches()) {
        LOG.warn("Saw subscription in v1beta1 format. Subscriptions should be in the format "
            + "projects/<project_id>/subscriptions/<subscription_name>");
        projectName = v1beta1Match.group(1);
        subscriptionName = v1beta1Match.group(2);
      } else {
        Matcher match = SUBSCRIPTION_REGEXP.matcher(path);
        if (!match.matches()) {
          throw new IllegalArgumentException("Pubsub subscription is not in "
              + "projects/<project_id>/subscriptions/<subscription_name> format: " + path);
        }
        projectName = match.group(1);
        subscriptionName = match.group(2);
      }

      validateProjectName(projectName);
      validatePubsubName(subscriptionName);
      return new PubsubSubscription(Type.NORMAL, projectName, subscriptionName);
    }

    /**
     * Returns the string representation of this subscription as a path used in the Cloud Pub/Sub
     * v1beta1 API.
     *
     * @deprecated the v1beta1 API for Cloud Pub/Sub is deprecated.
     */
    @Deprecated
    public String asV1Beta1Path() {
      if (type == Type.NORMAL) {
        return "/subscriptions/" + project + "/" + subscription;
      } else {
        return subscription;
      }
    }

    /**
     * Returns the string representation of this subscription as a path used in the Cloud Pub/Sub
     * v1beta2 API.
     *
     * @deprecated the v1beta2 API for Cloud Pub/Sub is deprecated.
     */
    @Deprecated
    public String asV1Beta2Path() {
      if (type == Type.NORMAL) {
        return "projects/" + project + "/subscriptions/" + subscription;
      } else {
        return subscription;
      }
    }

    /**
     * Returns the string representation of this subscription as a path used in the Cloud Pub/Sub
     * API.
     */
    public String asPath() {
      if (type == Type.NORMAL) {
        return "projects/" + project + "/subscriptions/" + subscription;
      } else {
        return subscription;
      }
    }
  }

  /**
   * Class representing a Cloud Pub/Sub Topic.
   */
  public static class PubsubTopic implements Serializable {
    private enum Type { NORMAL, FAKE }

    private final Type type;
    private final String project;
    private final String topic;

    private PubsubTopic(Type type, String project, String topic) {
      this.type = type;
      this.project = project;
      this.topic = topic;
    }

    /**
     * Creates a class representing a Cloud Pub/Sub topic from the specified topic path.
     *
     * <p>Cloud Pub/Sub topic names should be of the form
     * {@code /topics/<project>/<topic>}, where {@code <project>} is the name of
     * the publishing project. The {@code <topic>} component must comply with
     * the following requirements:
     *
     * <ul>
     * <li>Can only contain lowercase letters, numbers, dashes ('-'), underscores ('_') and periods
     * ('.').</li>
     * <li>Must be between 3 and 255 characters.</li>
     * <li>Must begin with a letter.</li>
     * <li>Must end with a letter or a number.</li>
     * <li>Cannot begin with 'goog' prefix.</li>
     * </ul>
     */
    public static PubsubTopic fromPath(String path) {
      if (path.equals(TOPIC_DEV_NULL_TEST_NAME)) {
        return new PubsubTopic(Type.FAKE, "", path);
      }

      String projectName, topicName;

      Matcher v1beta1Match = V1BETA1_TOPIC_REGEXP.matcher(path);
      if (v1beta1Match.matches()) {
        LOG.warn("Saw topic in v1beta1 format.  Topics should be in the format "
            + "projects/<project_id>/topics/<topic_name>");
        projectName = v1beta1Match.group(1);
        topicName = v1beta1Match.group(2);
      } else {
        Matcher match = TOPIC_REGEXP.matcher(path);
        if (!match.matches()) {
          throw new IllegalArgumentException(
              "Pubsub topic is not in projects/<project_id>/topics/<topic_name> format: " + path);
        }
        projectName = match.group(1);
        topicName = match.group(2);
      }

      validateProjectName(projectName);
      validatePubsubName(topicName);
      return new PubsubTopic(Type.NORMAL, projectName, topicName);
    }

    /**
     * Returns the string representation of this topic as a path used in the Cloud Pub/Sub
     * v1beta1 API.
     *
     * @deprecated the v1beta1 API for Cloud Pub/Sub is deprecated.
     */
    @Deprecated
    public String asV1Beta1Path() {
      if (type == Type.NORMAL) {
        return "/topics/" + project + "/" + topic;
      } else {
        return topic;
      }
    }

    /**
     * Returns the string representation of this topic as a path used in the Cloud Pub/Sub
     * v1beta2 API.
     *
     * @deprecated the v1beta2 API for Cloud Pub/Sub is deprecated.
     */
    @Deprecated
    public String asV1Beta2Path() {
      if (type == Type.NORMAL) {
        return "projects/" + project + "/topics/" + topic;
      } else {
        return topic;
      }
    }

    /**
     * Returns the string representation of this topic as a path used in the Cloud Pub/Sub
     * API.
     */
    public String asPath() {
      if (type == Type.NORMAL) {
        return "projects/" + project + "/topics/" + topic;
      } else {
        return topic;
      }
    }
  }

  /**
   * A {@link PTransform} that continuously reads from a Cloud Pub/Sub stream and
   * returns a {@link PCollection} of {@link String Strings} containing the items from
   * the stream.
   *
   * <p>When running with a {@link PipelineRunner} that only supports bounded
   * {@link PCollection PCollections} (such as {@link DirectPipelineRunner}),
   * only a bounded portion of the input Pub/Sub stream can be processed. As such, either
   * {@link Bound#maxNumRecords(int)} or {@link Bound#maxReadTime(Duration)} must be set.
   */
  public static class Read {
    /**
     * Creates and returns a transform for reading from Cloud Pub/Sub with the specified transform
     * name.
     */
    public static Bound<String> named(String name) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).named(name);
    }

    /**
     * Creates and returns a transform for reading from a Cloud Pub/Sub topic. Mutually exclusive
     * with {@link #subscription(String)}.
     *
     * <p>See {@link PubsubIO.PubsubTopic#fromPath(String)} for more details on the format
     * of the {@code topic} string.
     *
     * <p>Dataflow will start reading data published on this topic from the time the pipeline is
     * started. Any data published on the topic before the pipeline is started will not be read by
     * Dataflow.
     */
    public static Bound<String> topic(String topic) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).topic(topic);
    }

    /**
     * Creates and returns a transform for reading from a specific Cloud Pub/Sub subscription.
     * Mutually exclusive with {@link #topic(String)}.
     *
     * <p>See {@link PubsubIO.PubsubSubscription#fromPath(String)} for more details on the format
     * of the {@code subscription} string.
     */
    public static Bound<String> subscription(String subscription) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).subscription(subscription);
    }

    /**
     * Creates and returns a transform reading from Cloud Pub/Sub where record timestamps are
     * expected to be provided as Pub/Sub message attributes. The {@code timestampLabel}
     * parameter specifies the name of the attribute that contains the timestamp.
     *
     * <p>The timestamp value is expected to be represented in the attribute as either:
     *
     * <ul>
     * <li>a numerical value representing the number of milliseconds since the Unix epoch. For
     * example, if using the Joda time classes, {@link Instant#getMillis()} returns the correct
     * value for this attribute.
     * <li>a String in RFC 3339 format. For example, {@code 2015-10-29T23:41:41.123Z}. The
     * sub-second component of the timestamp is optional, and digits beyond the first three
     * (i.e., time units smaller than milliseconds) will be ignored.
     * </ul>
     *
     * <p>If {@code timestampLabel} is not provided, the system will generate record timestamps
     * the first time it sees each record. All windowing will be done relative to these timestamps.
     *
     * <p>By default, windows are emitted based on an estimate of when this source is likely
     * done producing data for a given timestamp (referred to as the Watermark; see
     * {@link AfterWatermark} for more details). Any late data will be handled by the trigger
     * specified with the windowing strategy &ndash; by default it will be output immediately.
     *
     * <p>Note that the system can guarantee that no late data will ever be seen when it assigns
     * timestamps by arrival time (i.e. {@code timestampLabel} is not provided).
     *
     * @see <a href="https://www.ietf.org/rfc/rfc3339.txt">RFC 3339</a>
     */
    public static Bound<String> timestampLabel(String timestampLabel) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).timestampLabel(timestampLabel);
    }

    /**
     * Creates and returns a transform for reading from Cloud Pub/Sub where unique record
     * identifiers are expected to be provided as Pub/Sub message attributes. The {@code idLabel}
     * parameter specifies the attribute name. The value of the attribute can be any string
     * that uniquely identifies this record.
     *
     * <p>If {@code idLabel} is not provided, Dataflow cannot guarantee that no duplicate data will
     * be delivered on the Pub/Sub stream. In this case, deduplication of the stream will be
     * strictly best effort.
     */
    public static Bound<String> idLabel(String idLabel) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).idLabel(idLabel);
    }

    /**
     * Creates and returns a transform for reading from Cloud Pub/Sub that uses the given
     * {@link Coder} to decode Pub/Sub messages into a value of type {@code T}.
     *
     * <p>By default, uses {@link StringUtf8Coder}, which just
     * returns the text lines as Java strings.
     *
     * @param <T> the type of the decoded elements, and the elements
     * of the resulting PCollection.
     */
    public static <T> Bound<T> withCoder(Coder<T> coder) {
      return new Bound<>(coder);
    }

    /**
     * Creates and returns a transform for reading from Cloud Pub/Sub with a maximum number of
     * records that will be read. The transform produces a <i>bounded</i> {@link PCollection}.
     *
     * <p>Either this option or {@link #maxReadTime(Duration)} must be set in order to create a
     * bounded source.
     */
    public static Bound<String> maxNumRecords(int maxNumRecords) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).maxNumRecords(maxNumRecords);
    }

    /**
     * Creates and returns a transform for reading from Cloud Pub/Sub with a maximum number of
     * duration during which records will be read.  The transform produces a <i>bounded</i>
     * {@link PCollection}.
     *
     * <p>Either this option or {@link #maxNumRecords(int)} must be set in order to create a bounded
     * source.
     */
    public static Bound<String> maxReadTime(Duration maxReadTime) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).maxReadTime(maxReadTime);
    }

    /**
     * A {@link PTransform} that reads from a Cloud Pub/Sub source and returns
     * a unbounded {@link PCollection} containing the items from the stream.
     */
    public static class Bound<T> extends PTransform<PInput, PCollection<T>> {
      /** The Cloud Pub/Sub topic to read from. */
      @Nullable private final PubsubTopic topic;

      /** The Cloud Pub/Sub subscription to read from. */
      @Nullable private final PubsubSubscription subscription;

      /** The name of the message attribute to read timestamps from. */
      @Nullable private final String timestampLabel;

      /** The name of the message attribute to read unique message IDs from. */
      @Nullable private final String idLabel;

      /** The coder used to decode each record. */
      @Nullable private final Coder<T> coder;

      /** Stop after reading this many records. */
      private final int maxNumRecords;

      /** Stop after reading for this much time. */
      @Nullable private final Duration maxReadTime;

      private Bound(Coder<T> coder) {
        this(null, null, null, null, coder, null, 0, null);
      }

      private Bound(String name, PubsubSubscription subscription, PubsubTopic topic,
          String timestampLabel, Coder<T> coder, String idLabel, int maxNumRecords,
          Duration maxReadTime) {
        super(name);
        this.subscription = subscription;
        this.topic = topic;
        this.timestampLabel = timestampLabel;
        this.coder = coder;
        this.idLabel = idLabel;
        this.maxNumRecords = maxNumRecords;
        this.maxReadTime = maxReadTime;
      }

      /**
       * Returns a transform that's like this one but with the given step name.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> named(String name) {
        return new Bound<>(
            name, subscription, topic, timestampLabel, coder, idLabel, maxNumRecords, maxReadTime);
      }

      /**
       * Returns a transform that's like this one but reading from the
       * given subscription.
       *
       * <p>See {@link PubsubIO.PubsubSubscription#fromPath(String)} for more details on the format
       * of the {@code subscription} string.
       *
       * <p>Multiple readers reading from the same subscription will each receive
       * some arbitrary portion of the data.  Most likely, separate readers should
       * use their own subscriptions.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> subscription(String subscription) {
        return new Bound<>(name, PubsubSubscription.fromPath(subscription), topic, timestampLabel,
            coder, idLabel, maxNumRecords, maxReadTime);
      }

      /**
       * Returns a transform that's like this one but that reads from the specified topic.
       *
       * <p>See {@link PubsubIO.PubsubTopic#fromPath(String)} for more details on the
       * format of the {@code topic} string.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> topic(String topic) {
        return new Bound<>(name, subscription, PubsubTopic.fromPath(topic), timestampLabel, coder,
            idLabel, maxNumRecords, maxReadTime);
      }

      /**
       * Returns a transform that's like this one but that reads message timestamps
       * from the given message attribute. See {@link PubsubIO.Read#timestampLabel(String)} for
       * more details on the format of the timestamp attribute.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> timestampLabel(String timestampLabel) {
        return new Bound<>(
            name, subscription, topic, timestampLabel, coder, idLabel, maxNumRecords, maxReadTime);
      }

      /**
       * Returns a transform that's like this one but that reads unique message IDs
       * from the given message attribute. See {@link PubsubIO.Read#idLabel(String)} for more
       * details on the format of the ID attribute.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> idLabel(String idLabel) {
        return new Bound<>(
            name, subscription, topic, timestampLabel, coder, idLabel, maxNumRecords, maxReadTime);
      }

      /**
       * Returns a transform that's like this one but that uses the given
       * {@link Coder} to decode each record into a value of type {@code X}.
       *
       * <p>Does not modify this object.
       *
       * @param <X> the type of the decoded elements, and the
       * elements of the resulting PCollection.
       */
      public <X> Bound<X> withCoder(Coder<X> coder) {
        return new Bound<>(
            name, subscription, topic, timestampLabel, coder, idLabel, maxNumRecords, maxReadTime);
      }

      /**
       * Returns a transform that's like this one but will only read up to the specified
       * maximum number of records from Cloud Pub/Sub. The transform produces a <i>bounded</i>
       * {@link PCollection}. See {@link PubsubIO.Read#maxNumRecords(int)} for more details.
       */
      public Bound<T> maxNumRecords(int maxNumRecords) {
        return new Bound<>(
            name, subscription, topic, timestampLabel, coder, idLabel, maxNumRecords, maxReadTime);
      }

      /**
       * Returns a transform that's like this one but will only read during the specified
       * duration from Cloud Pub/Sub. The transform produces a <i>bounded</i> {@link PCollection}.
       * See {@link PubsubIO.Read#maxReadTime(Duration)} for more details.
       */
      public Bound<T> maxReadTime(Duration maxReadTime) {
        return new Bound<>(
            name, subscription, topic, timestampLabel, coder, idLabel, maxNumRecords, maxReadTime);
      }

      @Override
      public PCollection<T> apply(PInput input) {
        if (topic == null && subscription == null) {
          throw new IllegalStateException("need to set either the topic or the subscription for "
              + "a PubsubIO.Read transform");
        }
        if (topic != null && subscription != null) {
          throw new IllegalStateException("Can't set both the topic and the subscription for a "
              + "PubsubIO.Read transform");
        }

        boolean boundedOutput = getMaxNumRecords() > 0 || getMaxReadTime() != null;

        if (boundedOutput) {
          return input.getPipeline().begin()
              .apply(Create.of((Void) null)).setCoder(VoidCoder.of())
              .apply(ParDo.of(new PubsubReader())).setCoder(coder);
        } else {
          return PCollection.<T>createPrimitiveOutputInternal(
                  input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED)
              .setCoder(coder);
        }
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);

        builder
            .addIfNotNull(DisplayData.item("timestampLabel", timestampLabel))
            .addIfNotNull(DisplayData.item("idLabel", idLabel))
            .addIfNotNull(DisplayData.item("maxReadTime", maxReadTime))
            .addIfNotDefault(DisplayData.item("maxNumRecords", maxNumRecords), 0);

        if (topic != null) {
          builder.add(DisplayData.item("topic", topic.asPath()));
        }

        if (subscription != null) {
          builder.add(DisplayData.item("subscription", subscription.asPath()));
        }
      }

      @Override
      protected Coder<T> getDefaultOutputCoder() {
        return coder;
      }

      public PubsubTopic getTopic() {
        return topic;
      }

      public PubsubSubscription getSubscription() {
        return subscription;
      }

      public String getTimestampLabel() {
        return timestampLabel;
      }

      public Coder<T> getCoder() {
        return coder;
      }

      public String getIdLabel() {
        return idLabel;
      }

      public int getMaxNumRecords() {
        return maxNumRecords;
      }

      public Duration getMaxReadTime() {
        return maxReadTime;
      }

      private class PubsubReader extends DoFn<Void, T> {
        private static final int DEFAULT_PULL_SIZE = 100;
        private static final int ACK_TIMEOUT_SEC = 60;

        @Override
        public void processElement(ProcessContext c) throws IOException {
          try (PubsubClient pubsubClient =
                   PubsubClient.newClient(TransportType.APIARY,
                       timestampLabel, idLabel, c.getPipelineOptions().as(PubsubOptions.class))) {

            PubsubClient.SubscriptionPath subscriptionPath;
            if (getSubscription() == null) {
              // Create a randomized subscription derived from the topic name.
              String subscription = getTopic().topic + "_dataflow_" + new Random().nextLong();
              subscriptionPath =
                  PubsubClient.subscriptionPathFromName(getTopic().project, subscription);
              try {
                pubsubClient.createSubscription(
                    PubsubClient.topicPathFromName(getTopic().project, getTopic().topic),
                    subscriptionPath,
                    ACK_TIMEOUT_SEC);
              } catch (Exception e) {
                throw new RuntimeException("Failed to create subscription: ", e);
              }
            } else {
              subscriptionPath = PubsubClient.subscriptionPathFromName(getSubscription().project,
                  getSubscription().subscription);
            }

            Instant endTime = (getMaxReadTime() == null)
                              ? new Instant(Long.MAX_VALUE) : Instant.now().plus(getMaxReadTime());

            List<IncomingMessage> messages = new ArrayList<>();

            Throwable finallyBlockException = null;
            try {
              while ((getMaxNumRecords() == 0 || messages.size() < getMaxNumRecords())
                     && Instant.now().isBefore(endTime)) {
                int batchSize = DEFAULT_PULL_SIZE;
                if (getMaxNumRecords() > 0) {
                  batchSize = Math.min(batchSize, getMaxNumRecords() - messages.size());
                }

                List<IncomingMessage> batchMessages =
                    pubsubClient.pull(System.currentTimeMillis(), subscriptionPath, batchSize,
                        false);
                List<String> ackIds = new ArrayList<>();
                for (IncomingMessage message : batchMessages) {
                  messages.add(message);
                  ackIds.add(message.ackId);
                }
                if (ackIds.size() != 0) {
                  pubsubClient.acknowledge(subscriptionPath, ackIds);
                }
              }
            } catch (IOException e) {
              throw new RuntimeException("Unexpected exception while reading from Pubsub: ", e);
            } finally {
              if (getSubscription() == null) {
                try {
                  pubsubClient.deleteSubscription(subscriptionPath);
                } catch (Exception e) {
                  finallyBlockException =
                      new RuntimeException("Failed to delete subscription: ", e);
                  LOG.error("Failed to delete subscription: ", e);
                }
              }
            }
            if (finallyBlockException != null) {
              Throwables.propagate(finallyBlockException);
            }

            for (IncomingMessage message : messages) {
              c.outputWithTimestamp(
                  CoderUtils.decodeFromByteArray(getCoder(), message.elementBytes),
                  new Instant(message.timestampMsSinceEpoch));
            }
          }
        }
      }
    }

    /** Disallow construction of utility class. */
    private Read() {}
  }


  /////////////////////////////////////////////////////////////////////////////

  /** Disallow construction of utility class. */
  private PubsubIO() {}

  /**
   * A {@link PTransform} that continuously writes a
   * {@link PCollection} of {@link String Strings} to a Cloud Pub/Sub stream.
   */
  // TODO: Support non-String encodings.
  public static class Write {
    /**
     * Creates a transform that writes to Pub/Sub with the given step name.
     */
    public static Bound<String> named(String name) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).named(name);
    }

    /**
     * Creates a transform that publishes to the specified topic.
     *
     * <p>See {@link PubsubIO.PubsubTopic#fromPath(String)} for more details on the format of the
     * {@code topic} string.
     */
    public static Bound<String> topic(String topic) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).topic(topic);
    }

    /**
     * Creates a transform that writes to Pub/Sub, adds each record's timestamp to the published
     * messages in an attribute with the specified name. The value of the attribute will be a number
     * representing the number of milliseconds since the Unix epoch. For example, if using the Joda
     * time classes, {@link Instant#Instant(long)} can be used to parse this value.
     *
     * <p>If the output from this sink is being read by another Dataflow source, then
     * {@link PubsubIO.Read#timestampLabel(String)} can be used to ensure the other source reads
     * these timestamps from the appropriate attribute.
     */
    public static Bound<String> timestampLabel(String timestampLabel) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).timestampLabel(timestampLabel);
    }

    /**
     * Creates a transform that writes to Pub/Sub, adding each record's unique identifier to the
     * published messages in an attribute with the specified name. The value of the attribute is an
     * opaque string.
     *
     * <p>If the the output from this sink is being read by another Dataflow source, then
     * {@link PubsubIO.Read#idLabel(String)} can be used to ensure that* the other source reads
     * these unique identifiers from the appropriate attribute.
     */
    public static Bound<String> idLabel(String idLabel) {
      return new Bound<>(DEFAULT_PUBSUB_CODER).idLabel(idLabel);
    }

    /**
     * Creates a transform that  uses the given {@link Coder} to encode each of the
     * elements of the input collection into an output message.
     *
     * <p>By default, uses {@link StringUtf8Coder}, which writes input Java strings directly as
     * records.
     *
     * @param <T> the type of the elements of the input PCollection
     */
    public static <T> Bound<T> withCoder(Coder<T> coder) {
      return new Bound<>(coder);
    }

    /**
     * A {@link PTransform} that writes an unbounded {@link PCollection} of {@link String Strings}
     * to a Cloud Pub/Sub stream.
     */
    public static class Bound<T> extends PTransform<PCollection<T>, PDone> {
      /** The Cloud Pub/Sub topic to publish to. */
      @Nullable private final PubsubTopic topic;
      /** The name of the message attribute to publish message timestamps in. */
      @Nullable private final String timestampLabel;
      /** The name of the message attribute to publish unique message IDs in. */
      @Nullable private final String idLabel;
      private final Coder<T> coder;

      private Bound(Coder<T> coder) {
        this(null, null, null, null, coder);
      }

      private Bound(
          String name, PubsubTopic topic, String timestampLabel, String idLabel, Coder<T> coder) {
        super(name);
        this.topic = topic;
        this.timestampLabel = timestampLabel;
        this.idLabel = idLabel;
        this.coder = coder;
      }

      /**
       * Returns a new transform that's like this one but with the specified step
       * name.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> named(String name) {
        return new Bound<>(name, topic, timestampLabel, idLabel, coder);
      }

      /**
       * Returns a new transform that's like this one but that writes to the specified
       * topic.
       *
       * <p>See {@link PubsubIO.PubsubTopic#fromPath(String)} for more details on the format of the
       * {@code topic} string.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> topic(String topic) {
        return new Bound<>(name, PubsubTopic.fromPath(topic), timestampLabel, idLabel, coder);
      }

      /**
       * Returns a new transform that's like this one but that publishes record timestamps
       * to a message attribute with the specified name. See
       * {@link PubsubIO.Write#timestampLabel(String)} for more details.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> timestampLabel(String timestampLabel) {
        return new Bound<>(name, topic, timestampLabel, idLabel, coder);
      }

      /**
       * Returns a new transform that's like this one but that publishes unique record IDs
       * to a message attribute with the specified name. See {@link PubsubIO.Write#idLabel(String)}
       * for more details.
       *
       * <p>Does not modify this object.
       */
      public Bound<T> idLabel(String idLabel) {
        return new Bound<>(name, topic, timestampLabel, idLabel, coder);
      }

      /**
       * Returns a new transform that's like this one
       * but that uses the given {@link Coder} to encode each of
       * the elements of the input {@link PCollection} into an
       * output record.
       *
       * <p>Does not modify this object.
       *
       * @param <X> the type of the elements of the input {@link PCollection}
       */
      public <X> Bound<X> withCoder(Coder<X> coder) {
        return new Bound<>(name, topic, timestampLabel, idLabel, coder);
      }

      @Override
      public PDone apply(PCollection<T> input) {
        if (topic == null) {
          throw new IllegalStateException("need to set the topic of a PubsubIO.Write transform");
        }
        input.apply(ParDo.of(new PubsubWriter()));
        return PDone.in(input.getPipeline());
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);

        builder
            .addIfNotNull(DisplayData.item("timestampLabel", timestampLabel))
            .addIfNotNull(DisplayData.item("idLabel", idLabel));

        if (topic != null) {
          builder.add(DisplayData.item("topic", topic.asPath()));
        }
      }

      @Override
      protected Coder<Void> getDefaultOutputCoder() {
        return VoidCoder.of();
      }

      public PubsubTopic getTopic() {
        return topic;
      }

      public String getTimestampLabel() {
        return timestampLabel;
      }

      public String getIdLabel() {
        return idLabel;
      }

      public Coder<T> getCoder() {
        return coder;
      }

      private class PubsubWriter extends DoFn<T, Void> {
        private static final int MAX_PUBLISH_BATCH_SIZE = 100;
        private transient List<OutgoingMessage> output;
        private transient PubsubClient pubsubClient;

        @Override
        public void startBundle(Context c) throws IOException {
          this.output = new ArrayList<>();
          this.pubsubClient =
              PubsubClient.newClient(TransportType.APIARY, timestampLabel, idLabel,
                  c.getPipelineOptions().as(PubsubOptions.class));
        }

        @Override
        public void processElement(ProcessContext c) throws IOException {
          OutgoingMessage message =
              new OutgoingMessage(CoderUtils.encodeToByteArray(getCoder(), c.element()),
                  c.timestamp().getMillis());
          output.add(message);

          if (output.size() >= MAX_PUBLISH_BATCH_SIZE) {
            publish();
          }
        }

        @Override
        public void finishBundle(Context c) throws IOException {
          if (!output.isEmpty()) {
            publish();
          }
          output = null;
          pubsubClient.close();
          pubsubClient = null;
        }

        private void publish() throws IOException {
          pubsubClient.publish(
              PubsubClient.topicPathFromName(getTopic().project, getTopic().topic),
              output);
          output.clear();
        }
      }
    }

    /** Disallow construction of utility class. */
    private Write() {}
  }
}
