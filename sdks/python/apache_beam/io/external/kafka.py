#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
  PTransforms for supporting Kafka in Python pipelines. These transforms do not
  run a Kafka client in Python. Instead, they expand to ExternalTransforms
  which the Expansion Service resolves to the Java SDK's KafkaIO. In other
  words: they are cross-language transforms.

  Note: To use these transforms, you need to start a Java Expansion Service.
  Please refer to the portability documentation on how to do that. Flink Users
  can use the built-in Expansion Service of the Flink Runner's Job Server. The
  expansion service address has to be provided when instantiating the
  transforms.

  If you start Flink's Job Server, the expansion service will be started on
  port 8097. This is also the configured default for this transform. For a
  different address, please set the expansion_service parameter.

  For more information see:
  - https://beam.apache.org/documentation/runners/flink/
  - https://beam.apache.org/roadmap/portability/
"""

from __future__ import absolute_import

from apache_beam.transforms.external import External
from apache_beam.coders import StrUtf8Coder
from apache_beam.coders import IterableCoder
from apache_beam.coders import TupleCoder
from apache_beam.coders.coders import LengthPrefixCoder


class ReadFromKafka(External):
  """
    An external PTransform which reads from Kafka and returns a KV pair for
    each item in the specified Kafka topics. If no Kafka Deserializer for
    key/value is provided, then the data will be returned as a raw byte array.

    Note: Runners need to support translating Read operations in order to use
    this source. At the moment only the Flink Runner supports this.

    Experimental; no backwards compatibility guarantees.
  """

  # Returns the key/value data as raw byte arrays
  byte_array_deserializer = 'org.apache.kafka.common.serialization.' \
                            'ByteArrayDeserializer'

  _urn = 'beam:external:java:kafka:read:v1'

  def __init__(self, consumer_config,
               topics,
               key_deserializer=byte_array_deserializer,
               value_deserializer=byte_array_deserializer,
               expansion_service=None):
    """
    Initializes a read operation from Kafka.

    :param consumer_config: A dictionary containing the consumer configuration.
    :param topics: A list of topic strings.
    :param key_deserializer: A fully-qualified Java class name of a Kafka
                             Deserializer for the topic's key, e.g.
                             'org.apache.kafka.common.
                             serialization.LongDeserializer'.
                             Default: 'org.apache.kafka.common.
                             serialization.ByteArrayDeserializer'.
    :param value_deserializer: A fully-qualified Java class name of a Kafka
                               Deserializer for the topic's value, e.g.
                               'org.apache.kafka.common.
                               serialization.LongDeserializer'.
                               Default: 'org.apache.kafka.common.
                               serialization.ByteArrayDeserializer'.
    :param expansion_service: The address (host:port) of the ExpansionService.
    """
    super(ReadFromKafka, self).__init__(expansion_service)
    self.consumer_config = consumer_config
    self.topics = topics
    self.key_deserializer = key_deserializer
    self.value_deserializer = value_deserializer

  def get_config_args(self):
    str_coder = LengthPrefixCoder(StrUtf8Coder())
    str_list_coder = IterableCoder(str_coder)
    map_coder = IterableCoder(TupleCoder([str_coder, str_coder]))
    return {
        'consumer_config':
            self.config_value(self.consumer_config.items(), map_coder),
        'topics':
            self.config_value(self.topics, str_list_coder),
        'key_deserializer':
            self.config_value(self.key_deserializer, str_coder),
        'value_deserializer':
            self.config_value(self.value_deserializer, str_coder),
    }


class WriteToKafka(External):
  """
    An external PTransform which writes KV data to a specified Kafka topic.
    If no Kafka Serializer for key/value is provided, then key/value are
    assumed to be byte arrays.

    Experimental; no backwards compatibility guarantees.
  """

  # Default serializer which passes raw bytes to Kafka
  byte_array_serializer = 'org.apache.kafka.common.serialization.' \
                          'ByteArraySerializer'

  _urn = 'beam:external:java:kafka:write:v1'

  def __init__(self, producer_config,
               topic,
               key_serializer=byte_array_serializer,
               value_serializer=byte_array_serializer,
               expansion_service=None):
    """
    Initializes a write operation to Kafka.

    :param consumer_config: A dictionary containing the producer configuration.
    :param topic: A Kafka topic name.
    :param key_deserializer: A fully-qualified Java class name of a Kafka
                             Serializer for the topic's key, e.g.
                             'org.apache.kafka.common.
                             serialization.LongSerializer'.
                             Default: 'org.apache.kafka.common.
                             serialization.ByteArraySerializer'.
    :param value_deserializer: A fully-qualified Java class name of a Kafka
                               Serializer for the topic's value, e.g.
                               'org.apache.kafka.common.
                               serialization.LongSerializer'.
                               Default: 'org.apache.kafka.common.
                               serialization.ByteArraySerializer'.
    :param expansion_service: The address (host:port) of the ExpansionService.
    """
    super(WriteToKafka, self).__init__(expansion_service)
    self.producer_config = producer_config
    self.topic = topic
    self.key_serializer = key_serializer
    self.value_serializer = value_serializer

  def get_config_args(self):
    str_coder = LengthPrefixCoder(StrUtf8Coder())
    map_coder = IterableCoder(TupleCoder([str_coder, str_coder]))
    return {
        'producer_config':
            self.config_value(self.producer_config.items(), map_coder),
        'topic':
            self.config_value(self.topic, str_coder),
        'key_serializer':
            self.config_value(self.key_serializer, str_coder),
        'value_serializer':
            self.config_value(self.value_serializer, str_coder),
    }
