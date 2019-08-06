# -*- coding: utf-8 -*-
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
from __future__ import print_function

import functools
import itertools
import pickle
import unittest
import uuid

import dill
import numpy as np
from nose.plugins.attrib import attr
from parameterized import parameterized

import apache_beam as beam
from apache_beam import transforms
from apache_beam.io.gcp import pubsub as beam_pubsub
from apache_beam.io.gcp.tests import utils as gcp_test_utils
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.interactive.caching import filebasedcache_it_test
from apache_beam.runners.interactive.caching import filebasedcache_test
from apache_beam.runners.interactive.caching import streambasedcache
from apache_beam.testing.extra_assertions import ExtraAssertionsMixin
from apache_beam.testing.test_pipeline import TestPipeline

# Protect against environments where the PubSub library is not available.
try:
  from google.cloud import pubsub_v1
except ImportError:
  pubsub_v1 = None


def read_directly(cache, limit=None, timeout=None, return_timestamp=False):
  """Read elements from cache using the cache API."""
  for element in itertools.islice(cache.read(timeout=timeout), limit):
    if return_timestamp:
      yield element
    else:
      yield element.value


def write_directly(cache, data_in):
  """Write elements to cache directly."""
  cache.write(data_in)


def write_through_pipeline(cache, data_in):
  """Write elements to cache using a Beam pipeline."""
  options = PipelineOptions(streaming=True)
  # Create is an "impulse source", so it terminates the streaming pipeline
  with beam.Pipeline(options=options) as p:
    _ = p | transforms.Create(data_in) | cache.writer()


def validate_directly(cache, expected):
  """Validate the contents of a cache by reading it directly."""
  output = list(itertools.islice(cache.read(timeout=10), len(expected)))
  actual = [o.value for o in output]
  test = TestCase("__init__")
  test.assertArrayCountEqual(actual, expected)


def validate_through_pipeline(cache, expected):
  """Validate the contents of a cache by writing it out into a Beam pipeline."""
  project, topic_name = beam_pubsub.parse_topic(cache.location)

  pub_client = pubsub_v1.PublisherClient()
  introspect_topic = pub_client.create_topic(
      pub_client.topic_path(project, topic_name + "-introspec"))

  sub_client = pubsub_v1.SubscriberClient()
  introspect_sub = sub_client.create_subscription(
      sub_client.subscription_path(project, topic_name + "-introspec"),
      introspect_topic.name,
      ack_deadline_seconds=60,
  )

  options = PipelineOptions(runner="DirectRunner", streaming=True)
  p = beam.Pipeline(options=options)
  pr = None
  try:
    _ = (
        p
        | "Read" >> cache.reader(with_attributes=True)
        | "Write" >> beam.io.WriteToPubSub(
            topic=introspect_topic.name, with_attributes=True))
    pr = p.run()
    output_messages = gcp_test_utils.read_from_pubsub(
        sub_client,
        introspect_sub.name,
        number_of_elements=len(expected),
        timeout=3 * 60,
        with_attributes=True)
    actual = [cache.coder.decode(message.data) for message in output_messages]
    test = TestCase("__init__")
    test.assertArrayCountEqual(actual, expected)
  finally:
    if pr is not None:
      pr.cancel()
    sub_client.delete_subscription(introspect_sub.name)
    pub_client.delete_topic(introspect_topic.name)


def retry_flakes(fn):

  @functools.wraps(fn)
  def resilient_fn(self, *args, **kwargs):
    num_tries = 0
    for _ in range(3):
      try:
        return fn(self, *args, **kwargs)
      except AssertionError as e:
        self.tearDown()
        self.setUp()
    raise e

  return resilient_fn


class TestCase(ExtraAssertionsMixin, unittest.TestCase):
  pass


@unittest.skipIf(pubsub_v1 is None, 'GCP dependencies are not installed.')
class PubSubBasedCacheSerializationTest(
    filebasedcache_it_test.SerializationTestBase, TestCase):

  cache_class = streambasedcache.PubSubBasedCache

  def setUp(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    project = test_pipeline.get_option('project')
    topic_name = "{}-{}".format(self.cache_class.__name__, uuid.uuid4().hex)
    self.location = pubsub_v1.PublisherClient.topic_path(project, topic_name)

  @parameterized.expand([("pickle", pickle), ("dill", dill)])
  @attr('IT')
  @retry_flakes
  def test_serde_empty(self, _, serializer):
    self.check_serde_empty(write_directly, read_directly, serializer)

  @parameterized.expand([("pickle", pickle), ("dill", dill)])
  @attr('IT')
  @retry_flakes
  def test_serde_filled(self, _, serializer):
    self.check_serde_filled(write_directly, read_directly, serializer)


@unittest.skipIf(pubsub_v1 is None, 'GCP dependencies are not installed.')
class PubSubBasedCacheRoundtripTest(filebasedcache_it_test.RoundtripTestBase,
                                    TestCase):

  cache_class = streambasedcache.PubSubBasedCache
  dataset = filebasedcache_test.GENERIC_TEST_DATA

  def setUp(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    project = test_pipeline.get_option('project')
    topic_name = "{}-{}".format(self.cache_class.__name__, uuid.uuid4().hex)
    self.location = pubsub_v1.PublisherClient.topic_path(project, topic_name)

  @parameterized.expand([
      ("{}-{}".format(
          write_fn.__name__,
          validate_fn.__name__,
      ), write_fn, validate_fn)
      for write_fn in [write_directly, write_through_pipeline]
      for validate_fn in [validate_directly, validate_through_pipeline]
  ])
  @attr('IT')
  @retry_flakes
  def test_roundtrip(self, _, write_fn, validate_fn):
    # Ideally, we would run this test with batches of different data types and
    # different coders. However, the test takes several seconds, so running it
    # for all possible writers, validators, and data types is not feasible.
    data = [
        None,
        100,
        1.23,
        "abc",
        b"def",
        u"±♠Ωℑ",
        ["d", 1],
        ("a", "b", "c"),
        (1, 2, 3, "b"),
        {"a": 1, 123: 1.234},
        np.zeros((3, 6)),
    ]
    self.check_roundtrip(write_fn, validate_fn, dataset=[data])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
