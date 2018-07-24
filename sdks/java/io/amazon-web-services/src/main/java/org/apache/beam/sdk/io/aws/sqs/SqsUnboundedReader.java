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
package org.apache.beam.sdk.io.aws.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.google.common.collect.Lists;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.joda.time.Instant;

class SqsUnboundedReader extends UnboundedSource.UnboundedReader<Message> {

  public static final int MAX_NUMBER_OF_MESSAGES = 10;
  private final SqsUnboundedSource source;
  private final AmazonSQS sqs;

  private Message current;
  private final Queue<Message> messagesNotYetRead;
  private Set<String> receiptHandlesToDelete;

  public SqsUnboundedReader(SqsUnboundedSource source, SqsCheckpointMark sqsCheckpointMark) {
    this.source = source;
    this.current = null;

    this.messagesNotYetRead = new ArrayDeque<>();
    receiptHandlesToDelete = new HashSet<>();

    final SqsConfiguration sqsConfiguration = source.getSqsConfiguration();
    sqs =
        AmazonSQSClientBuilder.standard()
            .withClientConfiguration(sqsConfiguration.getClientConfiguration())
            .withCredentials(sqsConfiguration.getAwsCredentialsProvider())
            .withRegion(sqsConfiguration.getAwsRegion())
            .build();

    if (sqsCheckpointMark != null) {
      if(sqsCheckpointMark.getReceiptHandlesToDelete() != null) {
        receiptHandlesToDelete.addAll(sqsCheckpointMark.getReceiptHandlesToDelete());
      }
    }
  }

  @Override
  public Instant getWatermark() {
    return Instant.now();
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return Instant.parse(current.getAttributes().get("Timestamp"));
  }

  @Override
  public Message getCurrent() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current;
  }

  @Override
  public CheckpointMark getCheckpointMark() {
    List<String> snapshotReceiptHandlesToDelete = Lists.newArrayList(receiptHandlesToDelete);
    return new SqsCheckpointMark(this, snapshotReceiptHandlesToDelete);
  }

  @Override
  public SqsUnboundedSource getCurrentSource() {
    return source;
  }

  @Override
  public boolean start() {
    return advance();
  }

  @Override
  public boolean advance() {
    if (messagesNotYetRead.isEmpty()) {
      pull();
    }

    current = messagesNotYetRead.poll();
    if (current == null) {
      return false;
    }

    receiptHandlesToDelete.add(current.getReceiptHandle());
    return true;
  }

  @Override
  public void close() {
    sqs.shutdown();
  }

  void delete(String receiptHandle) {
    sqs.deleteMessage(source.getRead().queueUrl(), receiptHandle);
    receiptHandlesToDelete.remove(receiptHandle);
  }

  private void pull() {
    final ReceiveMessageRequest receiveMessageRequest =
        new ReceiveMessageRequest(source.getRead().queueUrl());
    receiveMessageRequest.setMaxNumberOfMessages(MAX_NUMBER_OF_MESSAGES);
    final ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);

    final List<Message> messages = receiveMessageResult.getMessages();

    if (messages == null || messages.size() == 0) {
      return;
    }

    for (Message message : messages) {
      message.addAttributesEntry("Timestamp", Instant.now().toString());
      messagesNotYetRead.add(message);
    }
  }

  @Override
  public byte[] getCurrentRecordId() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current.getMessageId().getBytes(StandardCharsets.UTF_8);
  }
}
