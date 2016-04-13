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

package org.apache.beam.sdk.util;

import org.apache.beam.sdk.options.PubsubOptions;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * An (abstract) helper class for talking to Pubsub via an underlying transport.
 */
public abstract class PubsubClient implements AutoCloseable {
  /**
   * Which client to create for.
   */
  public enum TransportType {
    APIARY,
    GRPC;
  }

  /**
   * Project IDs must contain 6-63 lowercase letters, digits, or dashes.
   * IDs must start with a letter and may not end with a dash.
   * This regex isn't exact - this allows for patterns that would be rejected by
   * the service, but this is sufficient for basic parsing of table references.
   */
  private static final Pattern PROJECT_ID_REGEXP =
      Pattern.compile("[a-z][-a-z0-9:.]{4,61}[a-z0-9]");

  private static final Pattern PUBSUB_NAME_REGEXP = Pattern.compile("[a-zA-Z][-._~%+a-zA-Z0-9]+");

  private static final int PUBSUB_NAME_MIN_LENGTH = 3;
  private static final int PUBSUB_NAME_MAX_LENGTH = 255;

  private static void validateProjectId(String project) {
    if (!PROJECT_ID_REGEXP.matcher(project).matches()) {
      throw new IllegalArgumentException(
          "Illegal project name specified in Pubsub request: " + project);
    }
  }

  private static void validatePubsubName(String name) {
    if (name.length() < PUBSUB_NAME_MIN_LENGTH) {
      throw new IllegalArgumentException(
          "Pubsub topic or subscription name is shorter than 3 characters: " + name);
    }
    if (name.length() > PUBSUB_NAME_MAX_LENGTH) {
      throw new IllegalArgumentException(
          "Pubsub topic or subscription name is longer than 255 characters: " + name);
    }

    if (name.startsWith("goog")) {
      throw new IllegalArgumentException(
          "Pubsub topic or subscription name cannot start with goog: " + name);
    }

    Matcher match = PUBSUB_NAME_REGEXP.matcher(name);
    if (!match.matches()) {
      throw new IllegalArgumentException("Illegal Pubsub object name specified: " + name
                                         + " Please see Javadoc for naming rules.");
    }
  }

  /**
   * Path representing a cloud project id.
   */
  public static class ProjectPath implements Serializable {
    private final String path;

    public ProjectPath(String path) {
      this.path = path;
    }

    public String getPath() {
      return path;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ProjectPath that = (ProjectPath) o;

      return path.equals(that.path);

    }

    @Override
    public int hashCode() {
      return path.hashCode();
    }

    @Override
    public String toString() {
      return path;
    }
  }

  public static ProjectPath projectPathFromId(String projectId) {
    validateProjectId(projectId);
    return new ProjectPath(String.format("projects/%s", projectId));
  }

  /**
   * Path representing a Pubsub subscription.
   */
  public static class SubscriptionPath implements Serializable {
    private final String path;

    public SubscriptionPath(String path) {
      this.path = path;
    }

    public String getPath() {
      return path;
    }

    public String getV1Beta1Path() {
      String[] splits = path.split("/");
      Preconditions.checkState(splits.length == 4);
      return String.format("/subscriptions/%s/%s", splits[1], splits[3]);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SubscriptionPath that = (SubscriptionPath) o;
      return path.equals(that.path);
    }

    @Override
    public int hashCode() {
      return path.hashCode();
    }

    @Override
    public String toString() {
      return path;
    }
  }

  public static SubscriptionPath subscriptionPathFromName(
      String projectId, String subscriptionName) {
    validateProjectId(projectId);
    validatePubsubName(subscriptionName);
    return new SubscriptionPath(String.format("projects/%s/subscriptions/%s",
        projectId, subscriptionName));
  }

  /**
   * Path representing a Pubsub topic.
   */
  public static class TopicPath implements Serializable {
    private final String path;

    public TopicPath(String path) {
      this.path = path;
    }

    public String getPath() {
      return path;
    }

    public String getV1Beta1Path() {
      String[] splits = path.split("/");
      Preconditions.checkState(splits.length == 4);
      return String.format("/topics/%s/%s", splits[1], splits[3]);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TopicPath topicPath = (TopicPath) o;
      return path.equals(topicPath.path);
    }

    @Override
    public int hashCode() {
      return path.hashCode();
    }

    @Override
    public String toString() {
      return path;
    }
  }

  public static TopicPath topicPathFromName(String projectId, String topicName) {
    validateProjectId(projectId);
    validatePubsubName(topicName);
    return new TopicPath(String.format("projects/%s/topics/%s", projectId, topicName));
  }

  /**
   * A message to be sent to Pubsub.
   */
  public static class OutgoingMessage {
    /**
     * Underlying (encoded) element.
     */
    public final byte[] elementBytes;

    /**
     * Timestamp for element (ms since epoch).
     */
    public final long timestampMsSinceEpoch;

    public OutgoingMessage(byte[] elementBytes, long timestampMsSinceEpoch) {
      this.elementBytes = elementBytes;
      this.timestampMsSinceEpoch = timestampMsSinceEpoch;
    }
  }

  /**
   * A message received from Pubsub.
   */
  public static class IncomingMessage {
    /**
     * Underlying (encoded) element.
     */
    public final byte[] elementBytes;

    /**
     * Timestamp for element (ms since epoch). Either Pubsub's processing time,
     * or the custom timestamp associated with the message.
     */
    public final long timestampMsSinceEpoch;

    /**
     * Timestamp (in system time) at which we requested the message (ms since epoch).
     */
    public final long requestTimeMsSinceEpoch;

    /**
     * Id to pass back to Pubsub to acknowledge receipt of this message.
     */
    public final String ackId;

    /**
     * Id to pass to the runner to distinguish this message from all others.
     */
    public final byte[] recordId;

    public IncomingMessage(
        byte[] elementBytes,
        long timestampMsSinceEpoch,
        long requestTimeMsSinceEpoch,
        String ackId,
        byte[] recordId) {
      this.elementBytes = elementBytes;
      this.timestampMsSinceEpoch = timestampMsSinceEpoch;
      this.requestTimeMsSinceEpoch = requestTimeMsSinceEpoch;
      this.ackId = ackId;
      this.recordId = recordId;
    }
  }

  /**
   * Create a client using the underlying transport.
   */
  public static PubsubClient newClient(TransportType transportType,
                                       @Nullable String timestampLabel,
                                       @Nullable String idLabel,
                                       PubsubOptions options) throws IOException {
    switch (transportType) {
      case  APIARY:
        return PubsubApiaryClient.newClient(timestampLabel, idLabel, options);
      case GRPC:
        return PubsubGrpcClient.newClient(timestampLabel, idLabel, options);
    }
    throw new RuntimeException(); // cases are exhaustive.
  }

  /**
   * Gracefully close the underlying transport.
   */
  @Override
  public abstract void close();

  /**
   * Publish {@code outgoingMessages} to Pubsub {@code topic}. Return number of messages
   * published.
   *
   * @throws IOException
   */
  public abstract int publish(TopicPath topic, List<OutgoingMessage> outgoingMessages)
      throws IOException;

  /**
   * Request the next batch of up to {@code batchSize} messages from {@code subscription}.
   * Return the received messages, or empty collection if none were available. Does not
   * wait for messages to arrive. Returned messages will record their request time
   * as {@code requestTimeMsSinceEpoch}.
   *
   * @throws IOException
   */
  public abstract List<IncomingMessage> pull(
      long requestTimeMsSinceEpoch, SubscriptionPath subscription, int batchSize)
      throws IOException;

  /**
   * Acknowldege messages from {@code subscription} with {@code ackIds}.
   *
   * @throws IOException
   */
  public abstract void acknowledge(SubscriptionPath subscription, List<String> ackIds)
      throws IOException;

  /**
   * Modify the ack deadline for messages from {@code subscription} with {@code ackIds} to
   * be {@code deadlineSeconds} from now.
   *
   * @throws IOException
   */
  public abstract void modifyAckDeadline(
      SubscriptionPath subscription, List<String> ackIds, int deadlineSeconds)
      throws IOException;

  /**
   * Create {@code topic}.
   *
   * @throws IOException
   */
  public abstract void createTopic(TopicPath topic) throws IOException;

  /*
   * Delete {@code topic}.
   *
   * @throws IOException
   */
  public abstract void deleteTopic(TopicPath topic) throws IOException;

  /**
   * Return a list of topics for {@code project}.
   *
   * @throws IOException
   */
  public abstract List<TopicPath> listTopics(ProjectPath project) throws IOException;

  /**
   * Create {@code subscription} to {@code topic}.
   *
   * @throws IOException
   */
  public abstract void createSubscription(
      TopicPath topic, SubscriptionPath subscription, int ackDeadlineSeconds) throws IOException;

  /**
   * Delete {@code subscription}.
   *
   * @throws IOException
   */
  public abstract void deleteSubscription(SubscriptionPath subscription) throws IOException;

  /**
   * Return a list of subscriptions for {@code topic} in {@code project}.
   *
   * @throws IOException
   */
  public abstract List<SubscriptionPath> listSubscriptions(ProjectPath project, TopicPath topic)
      throws IOException;
}
