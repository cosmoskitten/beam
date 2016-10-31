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

from apache_beam.io.pubsub import PubSubSource, PubSubSink
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.transforms.display_test import make_nspace_display_data


class TestPubSubSource(unittest.TestCase):

  def test_display_data(self):
    source = PubSubSource('a_topic', 'a_subscription', 'a_label')
    nspace, dd = make_nspace_display_data(source)
    expected_items = [
        DisplayDataItem('a_topic', namespace=nspace, key='topic',
                        label='Pubsub Topic'),
        DisplayDataItem('a_subscription', namespace=nspace, key='subscription',
                        label='Pubsub Subscription'),
        DisplayDataItem('a_label', namespace=nspace, key='idLabel',
                        label='ID Label Attribute')]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_display_data_no_subscription(self):
    source = PubSubSource('a_topic')
    nspace, dd = make_nspace_display_data(source)
    expected_items = [
        DisplayDataItem('a_topic', namespace=nspace, key='topic',
                        label='Pubsub Topic')]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))


class TestPubSubSink(unittest.TestCase):
  def test_display_data(self):
    sink = PubSubSink('a_topic')
    nspace, dd = make_nspace_display_data(sink)
    expected_items = [
        DisplayDataItem('a_topic', namespace=nspace, key='topic',
                        label='Pubsub Topic')]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
