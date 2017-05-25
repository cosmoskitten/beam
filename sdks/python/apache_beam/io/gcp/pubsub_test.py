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

"""Unit tests for PubSub sources and sinks."""

import logging
import unittest

import hamcrest as hc

from apache_beam import coders
from apache_beam.io.gcp.pubsub import _PubSubSink
from apache_beam.io.gcp.pubsub import _PubSubSource
from apache_beam.io.gcp.pubsub import _PubSubReadPayloadTransformer
from apache_beam.io.gcp.pubsub import _PubSubWritePayloadTransformer
from apache_beam.io.gcp.pubsub import ReadStringsFromPubSub
from apache_beam.io.gcp.pubsub import WriteStringsToPubSub
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher


class TestReadStringsFromPubSub(unittest.TestCase):
  def test_expand(self):
    p = TestPipeline()
    pcoll = p | ReadStringsFromPubSub('a_topic', 'a_subscription', 'a_label')
    # Ensure that the output type is str
    self.assertEqual(str, pcoll.element_type)

    # Ensure that the type on the intermediate read output PCollection is bytes
    read_pcoll = pcoll.producer.inputs[0]
    self.assertEqual(bytes, read_pcoll.element_type)

    # Ensure that the properties passed through correctly
    source = read_pcoll.producer.transform.source
    self.assertEqual('a_topic', source.topic)
    self.assertEqual('a_subscription', source.subscription)
    self.assertEqual('a_label', source.id_label)


class TestWriteStringsToPubSub(unittest.TestCase):
  def test_expand(self):
    p = TestPipeline()
    pdone = p | ReadStringsFromPubSub('baz') | WriteStringsToPubSub('a_topic')

    # Ensure that the properties passed through correctly
    sink = pdone.producer.transform.sink
    self.assertEqual('a_topic', sink.topic)

    # Ensure that the type on the intermediate payload transformer output
    # PCollection is bytes
    write_pcoll = pdone.producer.inputs[0]
    self.assertEqual(str, write_pcoll.element_type)


class TestPubSubSource(unittest.TestCase):
  def test_display_data(self):
    source = _PubSubSource('a_topic', 'a_subscription', 'a_label')
    dd = DisplayData.create_from(source)
    expected_items = [
        DisplayDataItemMatcher('topic', 'a_topic'),
        DisplayDataItemMatcher('subscription', 'a_subscription'),
        DisplayDataItemMatcher('id_label', 'a_label')]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_display_data_no_subscription(self):
    source = _PubSubSource('a_topic')
    dd = DisplayData.create_from(source)
    expected_items = [
        DisplayDataItemMatcher('topic', 'a_topic')]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))


class TestPubSubSink(unittest.TestCase):
  def test_display_data(self):
    sink = _PubSubSink('a_topic')
    dd = DisplayData.create_from(sink)
    expected_items = [
        DisplayDataItemMatcher('topic', 'a_topic')]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))


class TestPubSubReadPayloadTransformer(unittest.TestCase):
  def test_transforming_payload_context_sensitive(self):
    test_data = b'test_data'
    encoded_test_data = coders.BytesCoder().encode(test_data)
    transformer = _PubSubReadPayloadTransformer(coders.BytesCoder())
    # transformer will double decode the bytes first in a nested context
    # removing the length prefix and a second time in an outer context
    self.assertEqual(test_data, transformer.transform_value(encoded_test_data))

  def test_transforming_payload_context_insensitive(self):
    test_data = 'test_data'
    encoded_test_data = coders.BytesCoder().encode(
        coders.StrUtf8Coder().encode(test_data))
    transformer = _PubSubReadPayloadTransformer(coders.StrUtf8Coder())
    # transformer will double decode the bytes first in a nested context
    # removing the length prefix and a second time in an outer context
    self.assertEqual(test_data, transformer.transform_value(encoded_test_data))


class TestPubSubWritePayloadTransformer(unittest.TestCase):
  def test_transforming_payload_context_sensitive(self):
    test_data = b'test_data'
    encoded_test_data = coders.BytesCoder().encode(test_data)
    transformer = _PubSubWritePayloadTransformer(coders.BytesCoder())
    # transformer will double encode the bytes first in an outer context
    # and then a second time in a nested context adding the length prefix
    self.assertEqual(encoded_test_data, transformer.transform_value(test_data))

  def test_transforming_payload_context_insensitive(self):
    test_data = 'test_data'
    encoded_test_data = coders.BytesCoder().encode(
        coders.StrUtf8Coder().encode(test_data))
    transformer = _PubSubWritePayloadTransformer(coders.StrUtf8Coder())
    # transformer will double encode the bytes first in an outer context
    # and then a second time in a nested context adding the length prefix
    self.assertEqual(encoded_test_data, transformer.transform_value(test_data))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
