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

import contextlib
import itertools
import json
import logging
import math
import os
import sys
import time
import unittest
import uuid
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import combiners
from apache_beam.transforms import window

try:
  # pylint: disable=ungrouped-imports
  from contextlib import ExitStack
except ImportError:
  from contextlib2 import ExitStack

try:
  from google.api_core import exceptions as gexc
  from google.cloud import pubsub_v1
except ImportError:
  gexc = None
  pubsub_v1 = None


@contextlib.contextmanager
def create_pubsub_topic(project_id, prefix):
  pub_client = pubsub_v1.PublisherClient()
  topic_path = pub_client.topic_path(project_id,
                                     "{}-{}".format(prefix,
                                                    uuid.uuid4().hex))
  pub_client.create_topic(topic_path)
  try:
    yield topic_path
  finally:
    pub_client.delete_topic(topic_path)


@contextlib.contextmanager
def create_pubsub_subscription(topic_path, suffix=""):
  sub_client = pubsub_v1.SubscriberClient()
  subscription_path = topic_path.replace(
      "/topics/", "/subscriptions/") + ("-{}".format(suffix) if suffix else "")
  sub_client.create_subscription(subscription_path, topic_path)
  try:
    yield subscription_path
  finally:
    sub_client.delete_subscription(subscription_path)


def write_to_pubsub(data_list, topic_path, attributes_fn=None, chunk_size=100):
  pub_client = pubsub_v1.PublisherClient()
  for start in range(0, len(data_list), chunk_size):
    data_chunk = data_list[start:start + chunk_size]
    if attributes_fn:
      attributes_chunk = [attributes_fn(data) for data in data_chunk]
    else:
      attributes_chunk = [{} for _ in data_chunk]
    futures = [
        pub_client.publish(topic_path,
                           json.dumps(data).encode("utf-8"), **attributes)
        for data, attributes in zip(data_chunk, attributes_chunk)
    ]
    for future in futures:
      future.result()
    time.sleep(0.1)


def read_from_pubsub(subscription_path, number_of_elements=None, timeout=None):
  if number_of_elements is None and timeout is None:
    raise ValueError(
        "At least one of {'number_of_elements', 'timeout'} must be specified.")
  sub_client = pubsub_v1.SubscriberClient()
  messages = []
  start_time = time.time()

  while True:
    if number_of_elements is not None and len(messages) >= number_of_elements:
      break
    if timeout is not None and (time.time() - start_time) >= timeout:
      break
    try:
      response = sub_client.pull(
          subscription_path, max_messages=1000, retry=None, timeout=1)
    except (gexc.RetryError, gexc.DeadlineExceeded):
      time.sleep(1)
      continue
    for msg in response.received_messages:
      sub_client.acknowledge(subscription_path, [msg.ack_id])
      data = json.loads(msg.message.data.decode("utf-8"))
      attributes = msg.message.attributes
      assert data["window_end_timestamp"] == int(attributes["timestamp"])
      messages.append(data)
  return messages


@contextlib.contextmanager
def run_pipeline(p):
  result = p.run()
  try:
    yield result
  finally:
    result.cancel()


def encode_to_pubsub_message(element):
  # pylint: disable=reimported
  import json
  from apache_beam.io.gcp.pubsub import PubsubMessage

  data = json.dumps(element).encode("utf-8")
  attributes = {"timestamp": str(element["window_end_timestamp"])}
  return PubsubMessage(data, attributes)


class AddWindowEndTimestamp(beam.DoFn):

  def process(self, element, window=beam.DoFn.WindowParam):
    element = {
        "window_start_timestamp": int(window.start.micros / 1000.0),
        "window_end_timestamp": int(window.end.micros / 1000.0),
        "elements": element,
    }
    yield element


class StreamingPipelineConsistencyBase(object):

  options = None

  if sys.version_info[0] < 3:

    def assertCountEqual(self, first, second, msg=None):
      """Assert that two containers have the same number of the same items in
            any order.
            """
      return self.assertItemsEqual(first, second, msg=msg)

  def setUp(self):
    self.maxDiff = None
    self._stack_context = ExitStack()
    self._stack = self._stack_context.__enter__()
    self.input_topic = self._stack.enter_context(
        create_pubsub_topic(
            self.options.view_as(GoogleCloudOptions).project, "input"))
    self.input_subscription = self._stack.enter_context(
        create_pubsub_subscription(self.input_topic))
    self.output_topic = self._stack.enter_context(
        create_pubsub_topic(
            self.options.view_as(GoogleCloudOptions).project, "output"))
    self.output_subscription = self._stack.enter_context(
        create_pubsub_subscription(self.output_topic))

  def tearDown(self):
    self._stack_context.__exit__(None, None, None)

  def start_pipeline(self):
    p = beam.Pipeline(options=self.options)
    # yapf: disable
    _ = (
        p
        | "Read" >> beam.io.ReadFromPubSub(
            subscription=self.input_subscription,
            with_attributes=True,
            timestamp_attribute="timestamp",
        )
        | "Decode" >> beam.Map(lambda e: json.loads(e.data.decode("utf-8")))
        | "Add windowing" >> beam.WindowInto(window.FixedWindows(1))
        | "Group into batches" >> beam.CombineGlobally(
            combiners.ToListCombineFn()).without_defaults()
        | "Add window end timestamp to each batch" >> beam.ParDo(
            AddWindowEndTimestamp())
        | "Encode" >> beam.Map(encode_to_pubsub_message)
        | "Write" >> beam.io.WriteToPubSub(
            self.output_topic, with_attributes=True)
    )
    # yapf: enable
    return self._stack.enter_context(run_pipeline(p))

  def test_discard_late_data(self):
    data = [{"input_timestamp": i * 1000} for i in range(1000, 900, -1)]
    write_to_pubsub(
        data,
        self.input_topic,
        attributes_fn=lambda e: {"timestamp": str(e["input_timestamp"])},
    )
    expected_output = [{
        "elements": [d],
        "window_start_timestamp": d["input_timestamp"],
        "window_end_timestamp": d["input_timestamp"] + 1000,
    } for d in data]
    self.start_pipeline()
    output = read_from_pubsub(
        self.output_subscription, number_of_elements=len(data))
    self.assertCountEqual(output, expected_output)

    current_time = (datetime.utcnow() -
                    datetime.fromtimestamp(0)).total_seconds()
    late_data = [{"input_timestamp": i * 1000} for i in range(900, 890, -1)]
    fresh_data = [{"input_timestamp": i * 1000} for i in [current_time]]
    write_to_pubsub(
        late_data + fresh_data,
        self.input_topic,
        attributes_fn=lambda e: {"timestamp": str(e["input_timestamp"])},
    )
    expected_output = [{
        "elements": [d],
        "window_start_timestamp": int(d["input_timestamp"] // 1000 * 1000),
        "window_end_timestamp": int(d["input_timestamp"] // 1000 * 1000) + 1000
    } for d in fresh_data]
    output = read_from_pubsub(self.output_subscription, number_of_elements=1)
    self.assertCountEqual(output, expected_output)

  def test_single_output_per_window(self):
    data = [{"input_timestamp": i * 100} for i in range(1000, 900, -1)]
    write_to_pubsub(
        data,
        self.input_topic,
        attributes_fn=lambda e: {"timestamp": str(e["input_timestamp"])},
    )

    def get_window_end_timestamp(element):
      # Assuming that windows are 1 second (1000 ms) long
      return int(element["input_timestamp"] / 1000.0) * 1000

    expected_output = [{
        "elements": sorted(g),
        "window_start_timestamp": k,
        "window_end_timestamp": k + 1000,
    } for k, g in itertools.groupby(data, key=get_window_end_timestamp)]

    self.start_pipeline()
    output = read_from_pubsub(
        self.output_subscription,
        number_of_elements=int(math.ceil(len(data) / 10)))
    for element in output:
      element["elements"] = sorted(element["elements"])
    self.assertCountEqual(output, expected_output)


class DirectRunnerStreamingPipelineConsistencyTest(
    StreamingPipelineConsistencyBase, unittest.TestCase):

  options = PipelineOptions(
      runner="DirectRunner",
      streaming=True,
      project=os.environ["PROJECT"],
  )


@unittest.skipIf(pubsub_v1 is None or gexc is None,
                 'GCP dependencies are not installed.')
@unittest.skipIf("PROJECT" not in os.environ,
                 'PROJECT and GCS_LOCATION environment variables must be set.')
class DataflowRunnerStreamingPipelineConsistencyTest(
    StreamingPipelineConsistencyBase, unittest.TestCase):

  options = PipelineOptions(
      runner="DataflowRunner",
      streaming=True,
      project=os.environ["PROJECT"],
      temp_location=os.environ["GCS_LOCATION"] + "/temp-it",
      sdk_location=os.path.expanduser(os.environ["SDK_LOCATION"])
      if "SDK_LOCATION" in os.environ else None,
  )

  def setUp(self):
    self.options.view_as(GoogleCloudOptions).job_name = "test-{}".format(
        uuid.uuid1().hex)
    super(DataflowRunnerStreamingPipelineConsistencyTest, self).setUp()


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
