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

"""Unittest for GCP testing utils."""

from __future__ import absolute_import

import logging
import unittest

import mock

from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.tests import utils
from apache_beam.testing import test_utils

# Protect against environments where bigquery library is not available.
try:
  from google.api_core import exceptions as gexc
  from google.cloud import bigquery
except ImportError:
  gexc = None
  bigquery = None


@unittest.skipIf(bigquery is None, 'Bigquery dependencies are not installed.')
@mock.patch.object(bigquery, 'Client')
class UtilsTest(unittest.TestCase):

  def setUp(self):
    test_utils.patch_retry(self, utils)

  @mock.patch.object(bigquery, 'Dataset')
  def test_create_bq_dataset(self, mock_dataset, mock_client):
    mock_client.dataset.return_value = 'dataset_ref'
    mock_dataset.return_value = 'dataset_obj'

    utils.create_bq_dataset('project', 'dataset_base_name')
    mock_client.return_value.create_dataset.assert_called_with('dataset_obj')

  def test_delete_bq_dataset(self, mock_client):
    utils.delete_bq_dataset('project', 'dataset_ref')
    mock_client.return_value.delete_dataset.assert_called_with(
        'dataset_ref', delete_contents=mock.ANY)

  def test_delete_table_succeeds(self, mock_client):
    mock_client.return_value.dataset.return_value.table.return_value = (
        'table_ref')

    utils.delete_bq_table('unused_project',
                          'unused_dataset',
                          'unused_table')
    mock_client.return_value.delete_table.assert_called_with('table_ref')

  def test_delete_table_fails_not_found(self, mock_client):
    mock_client.return_value.dataset.return_value.table.return_value = (
        'table_ref')
    mock_client.return_value.delete_table.side_effect = gexc.NotFound('test')

    with self.assertRaisesRegexp(Exception, r'does not exist:.*table_ref'):
      utils.delete_bq_table('unused_project',
                            'unused_dataset',
                            'unused_table')


class PubSubUtilTest(unittest.TestCase):

  def test_write_to_pubsub(self):
    mock_pubsub = mock.Mock()
    topic_path = "project/fakeproj/topics/faketopic"
    data = b'data'
    utils.write_to_pubsub(mock_pubsub, topic_path, [data])
    mock_pubsub.publish.assert_called_once_with(topic_path, data)
    mock_pubsub.publish.return_value.result.assert_called_once()

  def test_write_to_pubsub_with_attributes(self):
    mock_pubsub = mock.Mock()
    topic_path = "project/fakeproj/topics/faketopic"
    data = b'data'
    attributes = {'key': 'value'}
    message = PubsubMessage(data, attributes)
    utils.write_to_pubsub(
        mock_pubsub, topic_path, [message], with_attributes=True)
    mock_pubsub.publish.assert_called_once_with(topic_path, data, **attributes)
    mock_pubsub.publish.return_value.result.assert_called_once()

  def test_write_to_pubsub_delay(self):
    mock_pubsub = mock.Mock()
    topic_path = "project/fakeproj/topics/faketopic"
    data = b'data'
    with mock.patch('apache_beam.io.gcp.tests.utils.time') as mock_time:
      utils.write_to_pubsub(
          mock_pubsub, topic_path, [data], delay_between_chunks=123)
    mock_time.sleep.assert_called_once_with(123)
    mock_pubsub.publish.assert_called_once_with(topic_path, data)
    mock_pubsub.publish.return_value.result.assert_called_once()

  def test_read_from_pubsub(self):
    mock_pubsub = mock.Mock()
    subscription_path = "project/fakeproj/subscriptions/fakesub"
    data = b'data'
    ack_id = 'ack_id'
    pull_response = test_utils.create_pull_response(
        [test_utils.PullResponseMessage(data, ack_id=ack_id)])
    mock_pubsub.pull.return_value = pull_response
    output = utils.read_from_pubsub(
        mock_pubsub, subscription_path, number_of_elements=1)
    self.assertEqual([data], output)
    mock_pubsub.acknowledge.assert_called_once_with(subscription_path, [ack_id])

  def test_read_from_pubsub_with_attributes(self):
    mock_pubsub = mock.Mock()
    subscription_path = "project/fakeproj/subscriptions/fakesub"
    data = b'data'
    ack_id = 'ack_id'
    attributes = {'key': 'value'}
    message = PubsubMessage(data, attributes)
    pull_response = test_utils.create_pull_response(
        [test_utils.PullResponseMessage(data, attributes, ack_id=ack_id)])
    mock_pubsub.pull.return_value = pull_response
    output = utils.read_from_pubsub(
        mock_pubsub,
        subscription_path,
        with_attributes=True,
        number_of_elements=1)
    self.assertEqual([message], output)
    mock_pubsub.acknowledge.assert_called_once_with(subscription_path, [ack_id])

  def test_read_from_pubsub_flaky(self):
    number_of_elements = 10
    mock_pubsub = mock.Mock()
    subscription_path = "project/fakeproj/subscriptions/fakesub"
    data = b'data'
    ack_id = 'ack_id'
    pull_response = test_utils.create_pull_response(
        [test_utils.PullResponseMessage(data, ack_id=ack_id)])

    class FlakyPullResponse(object):

      def __init__(self, pull_response):
        self.pull_response = pull_response
        self._state = -1

      def __call__(self, *args, **kwargs):
        self._state += 1
        if self._state % 3 == 0:
          raise gexc.RetryError("", "")
        if self._state % 3 == 1:
          raise gexc.DeadlineExceeded("")
        if self._state % 3 == 2:
          return self.pull_response

    mock_pubsub.pull.side_effect = FlakyPullResponse(pull_response)
    output = utils.read_from_pubsub(
        mock_pubsub, subscription_path, number_of_elements=number_of_elements)
    self.assertEqual([data] * number_of_elements, output)
    self._assert_ack_ids_equal(mock_pubsub, [ack_id] * number_of_elements)

  def test_read_from_pubsub_many(self):
    response_size = 33
    number_of_elements = 100
    mock_pubsub = mock.Mock()
    subscription_path = "project/fakeproj/subscriptions/fakesub"
    data = b'data'
    attributes = {'key': 'value'}
    ack_id = 'ack_id'
    pull_response = test_utils.create_pull_response(
        [test_utils.PullResponseMessage(data, attributes, ack_id=ack_id)] *
        response_size)
    mock_pubsub.pull.return_value = pull_response
    output = utils.read_from_pubsub(
        mock_pubsub, subscription_path, number_of_elements=number_of_elements)
    self.assertGreaterEqual(len(output), number_of_elements)
    self.assertEqual([data] * len(output), output)
    self._assert_ack_ids_equal(mock_pubsub, [ack_id] * len(output))

  def test_read_from_pubsub_invalid_arg(self):
    sub_client = mock.Mock()
    subscription_path = "project/fakeproj/subscriptions/fakesub"
    with self.assertRaisesRegexp(ValueError, "number_of_elements"):
      utils.read_from_pubsub(sub_client, subscription_path)
    with self.assertRaisesRegexp(ValueError, "number_of_elements"):
      utils.read_from_pubsub(
          sub_client, subscription_path, with_attributes=True)

  def _assert_ack_ids_equal(self, mock_pubsub, ack_ids):
    actual_ack_ids = [
        ack_id for args_list in mock_pubsub.acknowledge.call_args_list
        for ack_id in args_list[0][1]
    ]
    self.assertEqual(actual_ack_ids, ack_ids)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
