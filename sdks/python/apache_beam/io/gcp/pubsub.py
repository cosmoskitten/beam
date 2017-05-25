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
"""Google Cloud PubSub sources and sinks.

Cloud Pub/Sub sources and sinks are currently supported only in streaming
pipelines, during remote execution.
"""

from __future__ import absolute_import

from apache_beam import coders
from apache_beam.coders.coder_impl import create_InputStream
from apache_beam.coders.coder_impl import create_OutputStream
from apache_beam.io.iobase import Read
from apache_beam.io.iobase import Write
from apache_beam.runners.dataflow.native_io import iobase as dataflow_io
from apache_beam.transforms import PTransform
from apache_beam.transforms import ParDo
from apache_beam.transforms.display import DisplayDataItem


__all__ = ['ReadStringsFromPubSub', 'WriteStringsToPubSub']


class ReadStringsFromPubSub(PTransform):
  """A ``PTransform`` for reading a string payload from Cloud Pub/Sub."""

  def __init__(self, topic, subscription=None, id_label=None):
    """Initializes ``ReadStringsFromPubSub``.

    Attributes:
      topic: Cloud Pub/Sub topic in the form "/topics/<project>/<topic>".
      subscription: Optional existing Cloud Pub/Sub subscription to use in the
        form "projects/<project>/subscriptions/<subscription>".
      id_label: The attribute on incoming Pub/Sub messages to use as a unique
        record identifier.  When specified, the value of this attribute (which
        can be any string that uniquely identifies the record) will be used for
        deduplication of messages.  If not provided, we cannot guarantee
        that no duplicate data will be delivered on the Pub/Sub stream. In this
        case, deduplication of the stream will be strictly best effort.
    """
    super(ReadStringsFromPubSub, self).__init__()
    self._source = _PubSubSource(
        topic,
        subscription=subscription,
        id_label=id_label)

  def expand(self, pvalue):
    pcoll = pvalue.pipeline | Read(self._source)
    pcoll.element_type = bytes
    pcoll = pcoll | 'parse payload' >> ParDo(
        _PubSubReadPayloadTransformer(coders.StrUtf8Coder()).transform_value)
    pcoll.element_type = str
    return pcoll


class WriteStringsToPubSub(PTransform):
  """A ``PTransform`` for writing string payloads to Cloud Pub/Sub."""

  def __init__(self, topic):
    """Initializes ``WriteStringsToPubSub``.

    Attributes:
      topic: Cloud Pub/Sub topic in the form "/topics/<project>/<topic>".
    """
    super(WriteStringsToPubSub, self).__init__()
    self._sink = _PubSubSink(topic)

  def expand(self, pcoll):
    pcoll = pcoll | 'format payload' >> ParDo(
        _PubSubWritePayloadTransformer(coders.StrUtf8Coder()).transform_value)
    pcoll.element_type = bytes
    return pcoll | Write(self._sink)


class _PubSubSource(dataflow_io.NativeSource):
  """Source for reading from a given Cloud Pub/Sub topic.

  Attributes:
    topic: Cloud Pub/Sub topic in the form "/topics/<project>/<topic>".
    subscription: Optional existing Cloud Pub/Sub subscription to use in the
      form "projects/<project>/subscriptions/<subscription>".
    id_label: The attribute on incoming Pub/Sub messages to use as a unique
      record identifier.  When specified, the value of this attribute (which can
      be any string that uniquely identifies the record) will be used for
      deduplication of messages.  If not provided, Dataflow cannot guarantee
      that no duplicate data will be delivered on the Pub/Sub stream. In this
      case, deduplication of the stream will be strictly best effort.
  """

  def __init__(self, topic, subscription=None, id_label=None):
    self.topic = topic
    self.subscription = subscription
    self.id_label = id_label

  @property
  def format(self):
    """Source format name required for remote execution."""
    return 'pubsub'

  def display_data(self):
    return {'id_label':
            DisplayDataItem(self.id_label,
                            label='ID Label Attribute').drop_if_none(),
            'topic':
            DisplayDataItem(self.topic,
                            label='Pubsub Topic'),
            'subscription':
            DisplayDataItem(self.subscription,
                            label='Pubsub Subscription').drop_if_none()}

  def reader(self):
    raise NotImplementedError(
        'PubSubSource is not supported in local execution.')


class _PubSubSink(dataflow_io.NativeSink):
  """Sink for writing to a given Cloud Pub/Sub topic."""

  def __init__(self, topic):
    self.topic = topic

  @property
  def format(self):
    """Sink format name required for remote execution."""
    return 'pubsub'

  def display_data(self):
    return {'topic': DisplayDataItem(self.topic, label='Pubsub Topic')}

  def writer(self):
    raise NotImplementedError(
        'PubSubSink is not supported in local execution.')


class _PubSubReadPayloadTransformer(object):
  """Converts from a payload of bytes encoded in the nested context to
  the type produced by the supplied coder when decoded in the outer context.
  """
  def __init__(self, value_coder):
    self._value_coder = value_coder

  def transform_value(self, nested_payload_bytes):
    payload_bytes = coders.BytesCoder().decode(nested_payload_bytes)
    return self._value_coder.get_impl().decode_from_stream(
        create_InputStream(payload_bytes),
        False)


class _PubSubWritePayloadTransformer(object):
  """Converts to a payload of bytes encoded in the nested context from
  the type produced by the supplied coder when encoded in the outer context.
  """
  def __init__(self, value_coder):
    self._value_coder = value_coder

  def transform_value(self, value):
    out = create_OutputStream()
    payload_bytes = self._value_coder.get_impl().encode_to_stream(
        value, out, False)
    return coders.BytesCoder().encode(out.get())