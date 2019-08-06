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

import contextlib
import functools
import logging
import time
import uuid
from datetime import datetime

import pytz
from future.moves import queue

import apache_beam as beam
import apache_beam.io.gcp.pubsub as beam_pubsub
from apache_beam import coders
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.runners.direct.direct_runner import _DirectWriteToPubSubFn
from apache_beam.runners.interactive.caching import PCollectionCache
from apache_beam.runners.interactive.caching.datatype_inference import infer_element_type
from apache_beam.transforms import PTransform
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp

try:
  from weakref import finalize
except ImportError:
  from backports.weakref import finalize

try:
  from google import api_core
  from google.cloud import pubsub
  from google.api_core import exceptions as gexc
except ImportError:
  api_core = None
  pubsub = None
  gexc = None

__all__ = [
    "StreamBasedCache",
    "PubSubBasedCache",
]


class StreamBasedCache(PCollectionCache):
  pass


class PubSubBasedCache(StreamBasedCache):

  _reader_class = ReadFromPubSub
  _writer_class = WriteToPubSub
  _reader_passthrough_arguments = {
      "id_label",
      "with_attributes",
      "timestamp_attribute",
  }
  requires_coder = True

  _default_timestamp_attribute = "ts"

  def __init__(self, location, mode="error", persist=False, **writer_kwargs):
    self.location = location
    self.coder = writer_kwargs.get("coder")
    self._writer_kwargs = writer_kwargs
    self._writer_kwargs["timestamp_attribute"] = self._writer_kwargs.get(
        "timestamp_attribute", self._default_timestamp_attribute)
    self._persist = persist
    self._timestamp = 0
    self._finalizers = []
    self._child_subscriptions = []
    self._primary_sub_exhausted = False

    if mode not in ["error", "append", "overwrite"]:
      raise ValueError("mode must be set to 'error' or 'append'.")

    # Initialize PubSub resources. Note that we cannot save pub_client and
    # sub_client as class attributes because they fail to serialize using
    # pickle.
    pub_client = pubsub.PublisherClient()
    sub_client = pubsub.SubscriberClient()

    self.project, self.topic_name = beam_pubsub.parse_topic(self.location)

    topic_path = pub_client.topic_path(self.project, self.topic_name)
    try:
      self.topic = pub_client.create_topic(topic_path)
    except gexc.AlreadyExists:
      if mode == "error":
        raise IOError("Topic '{}' already exists.".format(topic_path))
      elif mode == "overwrite":
        pub_client.delete_topic(topic_path)
        self.topic = pub_client.create_topic(topic_path)
      else:
        self.topic = pub_client.get_topic(topic_path)

    subscription_path = sub_client.subscription_path(self.project,
                                                     self.topic_name)
    try:
      self.subscription = sub_client.create_subscription(
          subscription_path, self.topic.name)
    except gexc.AlreadyExists:
      if mode == "error":
        raise IOError(
            "Subscription '{}' already exists.".format(subscription_path))
      elif mode == "overwrite":
        sub_client.delete_subscription(subscription_path)
        self.subscription = sub_client.create_subscription(
            subscription_path, self.topic.name)
      else:
        self.subscription = sub_client.get_subscription(subscription_path)

    snapshot_path = sub_client.snapshot_path(self.project, self.topic_name)
    try:
      self.snapshot = sub_client.create_snapshot(snapshot_path,
                                                 self.subscription.name)
    except gexc.AlreadyExists:
      if mode == "error":
        raise IOError("Snapshot '{}' already exists.".format(snapshot_path))
      elif mode == "overwrite":
        sub_client.delete_snapshot(snapshot_path)
        self.snapshot = sub_client.create_snapshot(snapshot_path,
                                                   self.subscription.name)
      else:
        # See: https://github.com/googleapis/google-cloud-python/issues/8554
        self.snapshot = pubsub.types.Snapshot()
        self.snapshot.name = snapshot_path
    except gexc.MethodNotImplemented:
      self.snapshot = None

    self._child_subscriptions.append(self.subscription)
    self._init_finalizers()

  @property
  def timestamp(self):
    return self._timestamp

  @property
  def persist(self):
    return self._persist

  @persist.setter
  def persist(self, persist):
    self._persist = persist
    self._init_finalizers()

  def reader(self, from_start=True, **kwargs):
    self._assert_topic_exists()

    reader_kwargs = self._reader_kwargs.copy()
    reader_kwargs.update(kwargs)

    if "subscription" not in reader_kwargs:
      reader_kwargs["subscription"] = self._create_child_subscription(
          seek_to_start=from_start).name

    reader = PatchedPubSubReader(self, self._reader_class, **reader_kwargs)
    return reader

  def writer(self):
    self._timestamp = time.time()
    writer = PatchedPubSubWriter(self, self._writer_class, (self.location,),
                                 self._writer_kwargs)
    return writer

  @contextlib.contextmanager
  def read_to_queue(self, seek_to_start=True, **kwargs):
    self._assert_topic_exists()

    reader_kwargs = self._reader_kwargs.copy()
    reader_kwargs.update(kwargs)

    if "subscription" in reader_kwargs:
      created_subsciption = None
    else:
      created_subsciption = self._create_child_subscription(
          seek_to_start=seek_to_start)
      reader_kwargs["subscription"] = created_subsciption.name
      assert created_subsciption in self._child_subscriptions

    @functools.total_ordering
    class PrioritizedTimestampedValue(TimestampedValue):

      def __lt__(self, other):
        return self.timestamp < other.timestamp

    # Set arbitrary queue size limit to prevent OOM errors.
    parsed_message_queue = queue.PriorityQueue(1000)
    decoder = DecodeFromPubSub(
        self.coder,
        with_attributes=reader_kwargs.get("with_attributes", False),
        timestamp_attribute=reader_kwargs["timestamp_attribute"])

    def callback(msg):
      msg.ack()
      message = PubsubMessage._from_message(msg)
      timestamped_value = next(decoder.process(message))
      ordered_timestamped_value = PrioritizedTimestampedValue(timestamped_value)
      parsed_message_queue.put(ordered_timestamped_value)

    sub_client = pubsub.SubscriberClient()
    future = sub_client.subscribe(
        reader_kwargs["subscription"], callback=callback)
    try:
      yield parsed_message_queue
    finally:
      future.cancel()
      if created_subsciption is not None:
        sub_client.delete_subscription(created_subsciption.name)
        self._child_subscriptions.remove(created_subsciption)

  def read(self, seek_to_start=True, delay=0, timeout=5, **kwargs):
    with self.read_to_queue(
        seek_to_start=seek_to_start, **kwargs) as message_queue:
      time.sleep(delay)
      while True:
        try:
          element = message_queue.get(timeout=timeout)
          yield element
        except queue.Empty:
          return

  def write(self, elements):
    if self.requires_coder and self.coder is None:
      # TODO(ostrokach): We might want to infer the element type from the first
      # N elements, rather than reading the entire iterator.
      elements = list(elements)
      element_type = infer_element_type(elements)
      self.coder = coders.registry.get_coder(element_type)

    writer_kwargs = self._writer_kwargs.copy()
    attributes_fn = writer_kwargs.pop("attributes_fn", None)
    # DirectRunner does not support timestamp_attribute
    timestamp_attribute = writer_kwargs.pop("timestamp_attribute")

    if writer_kwargs.get("with_attributes"):
      encoder = None
      if attributes_fn:
        raise ValueError(
            "Only one of with_attributes and attributes_fn can be provided.")
    else:
      writer_kwargs["with_attributes"] = True
      encoder = (
          EncodeToPubSubWithComputedAttributes(
              self.coder, attributes_fn, timestamp_attribute) if attributes_fn
          else EncodeToPubSubWithTimestamp(self.coder, timestamp_attribute))

    writer = self._writer_class(self.location, **writer_kwargs)

    do_fn = _DirectWriteToPubSubFn(writer._sink)
    do_fn.start_bundle()
    try:
      for message in elements:
        if encoder is not None:
          current_timestamp = Timestamp.from_utc_datetime(
              datetime.utcnow().replace(tzinfo=pytz.UTC))
          message = next(encoder.process(message, timestamp=current_timestamp))
        do_fn.process(message)
    finally:
      do_fn.finish_bundle()

  def truncate(self):
    self.coder = None
    self._primary_sub_exhausted = False
    sub_client = pubsub.SubscriberClient()
    try:
      sub_client.delete_subscription(self.subscription.name)
    except gexc.NotFound:
      pass
    _ = sub_client.create_subscription(self.subscription.name, self.location)
    if self.snapshot is not None:
      try:
        sub_client.delete_snapshot(self.snapshot.name)
      except gexc.NotFound:
        pass
      _ = sub_client.create_snapshot(self.snapshot.name, self.subscription.name)

  def remove(self):
    for finalizer in self._finalizers:
      finalizer()

  @property
  def removed(self):
    return not any(finalizer.alive for finalizer in self._finalizers)

  def __del__(self):
    self.remove()

  @property
  def _reader_kwargs(self):
    reader_kwargs = {
        k: v
        for k, v in self._writer_kwargs.items()
        if k in self._reader_passthrough_arguments
    }
    return reader_kwargs

  def _init_finalizers(self):
    if self._persist:
      self._finalizers = []
    else:
      pub_client = pubsub.PublisherClient()
      sub_client = pubsub.SubscriberClient()

      def delete_subscriptions(subscriptions):
        for sub in subscriptions:
          sub_client.delete_subscription(sub.name)

      self._finalizers = []
      if self.snapshot is not None:
        self._finalizers += [
            finalize(self, sub_client.delete_snapshot, self.snapshot.name)
        ]
      self._finalizers += [
          finalize(self, delete_subscriptions, self._child_subscriptions),
          finalize(self, pub_client.delete_topic, self.topic.name),
      ]

  def _assert_topic_exists(self):
    pub_client = pubsub.PublisherClient()
    try:
      _ = pub_client.get_topic(self.topic.name)
    except gexc.NotFound:
      raise IOError("Pubsub topic '{}' does not exist.".format(self.topic.name))

  def _create_child_subscription(self, seek_to_start=True):
    sub_client = pubsub.SubscriberClient()
    sub_path = None
    existing_sub_paths = [
        sub.name for sub in ([self.subscription] + self._child_subscriptions)
    ]
    while sub_path is None or sub_path in existing_sub_paths:
      sub_path = sub_client.subscription_path(
          self.project, self.topic_name + '-' + uuid.uuid4().hex)
    if not seek_to_start or self.snapshot is not None:
      subscription = sub_client.create_subscription(sub_path, self.location)
      self._child_subscriptions.append(subscription)
      if seek_to_start:
        sub_client.seek(
            subscription.name,
            snapshot=self.snapshot.name,
            retry=api_core.retry.Retry())
    elif not self._primary_sub_exhausted:
      subscription = self.subscription
      self._primary_sub_exhausted = True
    else:
      raise ValueError(
          "Cannot seek_to_start more than once in an environment where "
          "snapshots are not supported.")
    return subscription


class PatchedPubSubReader(PTransform):

  def __init__(self, cache, reader_class, *reader_args, **reader_kwargs):
    self._cache = cache
    self._reader_class = reader_class
    self._reader_args = reader_args
    self._reader_kwargs = reader_kwargs

    if "timestamp_attribute" not in reader_kwargs:
      raise ValueError(
          "timestamp_attribute must be specified when reading from cache.")

  def expand(self, pbegin):
    reader_kwargs = self._reader_kwargs.copy()

    reader = self._reader_class(*self._reader_args, **reader_kwargs)
    if reader_kwargs.get("with_attributes"):
      return pbegin | reader
    else:
      return pbegin | reader | beam.Map(self._cache.coder.decode)


class PatchedPubSubWriter(PTransform):

  def __init__(self, cache, writer_class, writer_args, writer_kwargs):
    self._cache = cache
    self._writer_class = writer_class
    self._writer_args = writer_args
    self._writer_kwargs = writer_kwargs

  def expand(self, pcoll):

    if self._cache.requires_coder and self._cache.coder is None:
      self._cache.coder = coders.registry.get_coder(pcoll.element_type)

    coder = self._cache.coder
    writer_kwargs = self._writer_kwargs.copy()
    attributes_fn = writer_kwargs.pop("attributes_fn", None)
    # DirectRunner does not support timestamp_attribute
    timestamp_attribute = writer_kwargs.pop("timestamp_attribute", "ts")

    if writer_kwargs.get("with_attributes"):
      writer = self._writer_class(*self._writer_args, **writer_kwargs)
      return pcoll | writer
    else:
      # Encode the element as a PubsubMessage ourselves
      writer_kwargs["with_attributes"] = True
      # DirectRunner does not support timestamp_attribute
      timestamp_attribute = writer_kwargs.pop("timestamp_attribute", "ts")
      encoder = (
          EncodeToPubSubWithComputedAttributes(
              coder, attributes_fn, timestamp_attribute) if attributes_fn else
          EncodeToPubSubWithTimestamp(coder, timestamp_attribute))
      writer = self._writer_class(*self._writer_args, **writer_kwargs)
      return pcoll | beam.ParDo(encoder) | writer


class DecodeFromPubSub(beam.DoFn):

  def __init__(self, coder, timestamp_attribute):
    super(DecodeFromPubSub, self).__init__()
    self.coder = coder
    self.timestamp_attribute = timestamp_attribute

  def process(self, message):
    # pylint: disable=reimported
    from apache_beam.transforms.window import TimestampedValue
    from apache_beam.utils.timestamp import Timestamp

    element = self.coder.decode(message.data)
    rfc3339_or_milli = message.attributes[self.timestamp_attribute]
    try:
      timestamp = Timestamp(micros=int(rfc3339_or_milli) * 1000)
    except ValueError:
      timestamp = Timestamp.from_rfc3339(rfc3339_or_milli)
    yield TimestampedValue(element, timestamp)


class EncodeToPubSubWithComputedAttributes(beam.DoFn):

  def __init__(self, coder, attributes_fn, timestamp_attribute):
    super(EncodeToPubSubWithComputedAttributes, self).__init__()
    self.coder = coder
    self.attributes_fn = attributes_fn
    self.timestamp_attribute = timestamp_attribute

  def process(self, element, timestamp=beam.DoFn.TimestampParam):
    # pylint: disable=reimported
    from apache_beam.io.gcp.pubsub import PubsubMessage

    attributes = self.attributes_fn(element)
    if self.timestamp_attribute not in attributes:
      if self.timestamp_attribute == "ts":
        attributes["ts"] = str(int(timestamp.micros / 1000.0))
      else:
        raise ValueError("Provided attributes_fn did not produce the expected "
                         "timestamp_attribute '{}'.".format(
                             self.timestamp_attribute))

    element_bytes = self.coder.encode(element)
    message = PubsubMessage(element_bytes, attributes)
    yield message


class EncodeToPubSubWithTimestamp(beam.DoFn):

  def __init__(self, coder, timestamp_attribute):
    self.coder = coder
    self.timestamp_attribute = timestamp_attribute

  def process(self, element, timestamp=beam.DoFn.TimestampParam):
    # pylint: disable=reimported
    from apache_beam.io.gcp.pubsub import PubsubMessage

    attributes = {self.timestamp_attribute: str(int(timestamp.micros / 1000.0))}
    element_bytes = self.coder.encode(element)
    message = PubsubMessage(element_bytes, attributes)
    yield message
