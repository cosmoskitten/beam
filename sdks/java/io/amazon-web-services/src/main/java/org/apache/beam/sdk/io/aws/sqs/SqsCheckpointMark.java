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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.UnboundedSource;

class SqsCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

  private List<String> receiptHandlesToDelete;
  private transient SqsUnboundedReader reader;

  private SqsCheckpointMark() {
    this.receiptHandlesToDelete = new ArrayList<>();
  }

  public SqsCheckpointMark(SqsUnboundedReader reader, List<String> snapshotReceiptHandlesToDelete) {

    this.reader = reader;
    this.receiptHandlesToDelete = snapshotReceiptHandlesToDelete;
  }

  @Override
  public void finalizeCheckpoint() {
    for (String receiptHandle : receiptHandlesToDelete) {
      reader.delete(receiptHandle);
    }
  }

  List<String> getReceiptHandlesToDelete() {
    return receiptHandlesToDelete;
  }
}
