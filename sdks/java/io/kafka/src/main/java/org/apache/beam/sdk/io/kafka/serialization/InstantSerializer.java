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

package org.apache.beam.sdk.io.kafka.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.joda.time.Instant;

/**
 * Kafka {@link Serializer} for {@link Instant}.
 *
 * <p>This uses the same encoding as {@link org.apache.beam.sdk.coders.InstantCoder}.
 */
public class InstantSerializer implements Serializer<Instant> {
    private LongSerializer longSerializer = null;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (longSerializer == null) {
            longSerializer = new LongSerializer();
        }
    }

    @Override
    public byte[] serialize(String topic, Instant instant) {
        return longSerializer.serialize(topic, instant.getMillis() - Long.MIN_VALUE);
    }

    @Override
    public void close() {
        longSerializer.close();
    }
};
