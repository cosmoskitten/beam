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

from __future__ import absolute_import

from apache_beam import ExternalTransform
from apache_beam import pvalue
from apache_beam.coders import BytesCoder
from apache_beam.coders import IterableCoder
from apache_beam.coders import TupleCoder
from apache_beam.coders.coders import LengthPrefixCoder
from apache_beam.portability.api.external_transforms_pb2 import ConfigValue
from apache_beam.portability.api.external_transforms_pb2 import ExternalConfigurationPayload
from apache_beam.transforms import ptransform


class ReadFromKafka(ptransform.PTransform):
  """
    A PTransform which from Kafka topics.
  """

  def __init__(self, consumer_config,
               topics,
               key_deserializer,
               value_deserializer,
               expansion_service=None):
    super(ReadFromKafka, self).__init__()
    self._urn = 'beam:external:java:kafka:read:v1'
    self.consumer_config = consumer_config
    self.topics = topics
    self.key_deserializer = key_deserializer
    self.value_deserializer = value_deserializer
    self.expansion_service = expansion_service

  def expand(self, pbegin):
    if not isinstance(pbegin, pvalue.PBegin):
      raise Exception("GenerateSequence must be a root transform")

    args = {
        'consumer_config':
            KafkaRead._encode_map(self.consumer_config),
        'topics':
            KafkaRead._encode_list(self.topics),
        'key_deserializer':
            KafkaRead._encode_str(self.key_deserializer),
        'value_deserializer':
            KafkaRead._encode_str(self.value_deserializer),
    }

    payload = ExternalConfigurationPayload(configuration=args)
    return pbegin.apply(
        ExternalTransform(
            self._urn,
            payload.SerializeToString(),
            self.expansion_service))

  @staticmethod
  def _encode_map(dict_obj):
    kv_list = [(key.encode('utf-8'), val.encode('utf-8'))
               for key, val in dict_obj.items()]
    coder = IterableCoder(TupleCoder(
        [LengthPrefixCoder(BytesCoder()), LengthPrefixCoder(BytesCoder())]))
    coder_urns = ['beam:coder:iterable:v1',
                  'beam:coder:kv:v1',
                  'beam:coder:bytes:v1',
                  'beam:coder:bytes:v1']
    return ConfigValue(
        coder_urn=coder_urns,
        payload=coder.encode(kv_list))

  @staticmethod
  def _encode_list(list_obj):
    encoded_list = [val.encode('utf-8') for val in list_obj]
    coder = IterableCoder(LengthPrefixCoder(BytesCoder()))
    coder_urns = ['beam:coder:iterable:v1',
                  'beam:coder:bytes:v1']
    return ConfigValue(
        coder_urn=coder_urns,
        payload=coder.encode(encoded_list))

  @staticmethod
  def _encode_str(str_obj):
    encoded_str = str_obj.encode('utf-8')
    coder = LengthPrefixCoder(BytesCoder())
    coder_urns = ['beam:coder:bytes:v1']
    return ConfigValue(
        coder_urn=coder_urns,
        payload=coder.encode(encoded_str))
