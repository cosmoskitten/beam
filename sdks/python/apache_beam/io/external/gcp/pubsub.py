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
from apache_beam.coders import FastPrimitivesCoder
from apache_beam.coders.coders import LengthPrefixCoder
from apache_beam.portability.api.external_transforms_pb2 import ConfigValue
from apache_beam.portability.api.external_transforms_pb2 import ExternalConfigurationPayload
from apache_beam.transforms import ptransform


class ReadFromPubSub(ptransform.PTransform):
  """An external ``PTransform`` for reading from Cloud Pub/Sub."""

  _urn = 'beam:external:java:pubsub:read:v1'

  def __init__(self, topic=None, subscription=None, id_label=None,
               with_attributes=False, timestamp_attribute=None,
               expansion_service='localhost:8097'):
    super(ReadFromPubSub, self).__init__()
    self.topic = topic
    self.subscription = subscription
    self.id_label = id_label
    self.with_attributes = with_attributes
    self.timestamp_attribute = timestamp_attribute
    self.expansion_service = expansion_service

  def expand(self, pbegin):
    if not isinstance(pbegin, pvalue.PBegin):
      raise Exception("ReadFromPubSub must be a root transform")

    args = {}

    if self.topic is not None:
      args['topic'] = _encode_str(self.topic)

    if self.subscription is not None:
      args['subscription'] = _encode_str(self.subscription)

    if self.id_label is not None:
      args['id_label'] = _encode_str(self.id_label)

    # FIXME: how do we encode a bool so that Java can decode it?
    # args['with_attributes'] = _encode_bool(self.with_attributes)

    if self.timestamp_attribute is not None:
      args['timestamp_attribute'] = _encode_str(self.timestamp_attribute)

    payload = ExternalConfigurationPayload(configuration=args)
    return pbegin.apply(
        ExternalTransform(
            self._urn,
            payload.SerializeToString(),
            self.expansion_service))


class WriteToPubSub(ptransform.PTransform):
  """An external ``PTransform`` for writing messages to Cloud Pub/Sub."""

  _urn = 'beam:external:java:pubsub:write:v1'

  def __init__(self, topic, with_attributes=False, id_label=None,
               timestamp_attribute=None):
    """Initializes ``WriteToPubSub``.

    Args:
      topic: Cloud Pub/Sub topic in the form "/topics/<project>/<topic>".
      with_attributes:
        True - input elements will be :class:`~PubsubMessage` objects.
        False - input elements will be of type ``bytes`` (message
        data only).
      id_label: If set, will set an attribute for each Cloud Pub/Sub message
        with the given name and a unique value. This attribute can then be used
        in a ReadFromPubSub PTransform to deduplicate messages.
      timestamp_attribute: If set, will set an attribute for each Cloud Pub/Sub
        message with the given name and the message's publish time as the value.
    """
    super(WriteToPubSub, self).__init__()
    self.topic = topic
    self.with_attributes = with_attributes
    self.id_label = id_label
    self.timestamp_attribute = timestamp_attribute

  def expand(self, pvalue):

    args = {
      'topic': _encode_str(self.topic)
    }

    if self.id_label is not None:
      args['id_label'] = _encode_str(self.id_label)

    # FIXME: how do we encode a bool so that Java can decode it?
    # args['with_attributes'] = _encode_bool(self.with_attributes)

    if self.timestamp_attribute is not None:
      args['timestamp_attribute'] = _encode_str(self.timestamp_attribute)

    payload = ExternalConfigurationPayload(configuration=args)
    return pvalue.apply(
        ExternalTransform(
            self._urn,
            payload.SerializeToString(),
            self.expansion_service))


def _encode_str(str_obj):
  encoded_str = str_obj.encode('utf-8')
  coder = LengthPrefixCoder(BytesCoder())
  coder_urns = ['beam:coder:bytes:v1']
  return ConfigValue(
      coder_urn=coder_urns,
      payload=coder.encode(encoded_str))
