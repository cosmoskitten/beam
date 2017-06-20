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

import static org.junit.Assert.assertTrue;

import java.util.Objects;

import org.apache.beam.sdk.util.CoderUtils;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.junit.Test;

/**
 * Test on the {@link AmqpMessageCoder}.
 */
public class AmqpMessageCoderTest {

  private final AmqpMessageCoder coder = AmqpMessageCoder.of();

  @Test
  public void testEncodeDecode() throws Exception {
    AmqpMessage message = new AmqpMessage();
    message.getMessage().setBody(new AmqpValue("test"));
    byte[] encoded = CoderUtils.encodeToByteArray(coder, message);
    AmqpMessage clone = CoderUtils.decodeFromByteArray(coder, encoded);

    assertTrue(Objects.equals(message, clone));
    assertTrue(Objects.equals(clone, message));
  }

}
