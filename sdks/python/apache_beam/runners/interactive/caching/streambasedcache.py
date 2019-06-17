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
import re
import time
import uuid
from datetime import datetime

import pytz
from future.moves import queue

import apache_beam as beam
import apache_beam.io.gcp.pubsub as beam_pubsub
from apache_beam import coders
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.runners.direct.direct_runner import _DirectWriteToPubSubFn
from apache_beam.runners.interactive.caching import PCollectionCache
from apache_beam.runners.interactive.caching.datatype_inference import infer_element_type
from apache_beam.transforms import PTransform
from apache_beam.utils.timestamp import Timestamp

try:
  from weakref import finalize
except ImportError:
  from backports.weakref import finalize

try:
  from google import api_core
  from google.cloud import pubsub_v1
  from google.api_core import exceptions as gexc
except ImportError:
  pubsub = None

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

  def __init__(self,
               location,
               mode="error",
               single_use=False,
               persist=False,
               **writer_kwargs):
    self.location = location
    self._mode = mode
    self._single_use = single_use
    self._persist = persist
    self.coder = writer_kwargs.pop("coder", None)
    self._writer_kwargs = writer_kwargs

    self._timestamp = 0
    self._num_reads = 0
    self._finalizer = (lambda: None) if persist else finalize(
        self, remove_topic_and_descendants, self.location)

    _, project_id, _, topic_name = self.location.split('/')
    self.subscription_path = pubsub_v1.SubscriberClient.subscription_path(
        project_id, topic_name)
    self.snapshot_path = pubsub_v1.SubscriberClient.snapshot_path(
        project_id, topic_name) if not self._single_use else None

    if re.search("projects/.+/topics/.+", location) is None:
      raise ValueError(
          "location must be the path to a pubsub subscription in the form: "
          "'projects/{project}/topics/{topic}'.")
    if mode not in ['error', 'append', 'overwrite']:
      raise ValueError(
          "mode must be set to one of: ['error', 'append', 'overwrite'].")

    self._init_pubsub_resources()

  @property
  def timestamp(self):
    return self._timestamp

  @property
  def persist(self):
    return self._persist

  @persist.setter
  def persist(self, persist):
    if persist != self._persist:
      self._persist = persist
      self._finalizer = (lambda: None) if persist else finalize(
          self, remove_topic_and_descendants, self.location)

  def reader(self, from_start=True, **kwargs):
    self._assert_topic_exists()
    self._assert_read_valid()
    self._num_reads += 1

    reader_kwargs = self._reader_kwargs.copy()
    reader_kwargs.update(kwargs)

    if "subscription" not in reader_kwargs:
      if from_start:
        if self._single_use:
          reader_kwargs["subscription"] = self.subscription_path
        else:
          reader_kwargs["subscription"] = self._create_new_subscription(
              self.location, self.snapshot_path)
      else:
        reader_kwargs["subscription"] = self._create_new_subscription(
            self.location)

    reader = PatchedPubSubReader(self, self._reader_class, (), reader_kwargs)
    return reader

  def writer(self):
    self._timestamp = time.time()
    writer = PatchedPubSubWriter(self, self._writer_class, (self.location,),
                                 self._writer_kwargs)
    return writer

  @contextlib.contextmanager
  def read_to_queue(self, from_start=True, **kwargs):
    self._assert_topic_exists()
    self._assert_read_valid()
    self._num_reads += 1

    reader_kwargs = self._reader_kwargs.copy()
    reader_kwargs.update(kwargs)

    @functools.total_ordering
    class PrioritizedTimestampedValue(object):

      def __init__(self, element):
        self.value = element.value
        self.timestamp = element.timestamp

      def __lt__(self, other):
        return self.timestamp < other.timestamp

      def __eq__(self, other):
        return self.timestamp == other.timestamp

    # Set arbitrary queue size limit to prevent OOM errors.
    parsed_message_queue = queue.PriorityQueue(1000)
    decoder = DecodeFromPubSub(self.coder,
                               reader_kwargs.get("timestamp_attribute", "ts"))

    def callback(message):
      message.ack()
      timestamped_value = next(decoder.process(message))
      ordered_timestamped_value = PrioritizedTimestampedValue(timestamped_value)
      parsed_message_queue.put(ordered_timestamped_value)

    sub_client = pubsub_v1.SubscriberClient()
    subscription_path = self._get_or_create_subscription(
        kwargs.get("subscription"),
        snapshot_path=self.snapshot_path if from_start else None)
    future = sub_client.subscribe(subscription_path, callback=callback)
    try:
      yield parsed_message_queue
    finally:
      future.cancel()
      sub_client.delete_subscription(subscription_path)

  def read(self, from_start=True, burnin_period=0, timeout=5, **kwargs):
    with self.read_to_queue(from_start=from_start, **kwargs) as message_queue:
      time.sleep(burnin_period)
      while True:
        try:
          element = message_queue.get(timeout=timeout)
          yield element.timestamp, element.value
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
    timestamp_attribute = writer_kwargs.pop("timestamp_attribute", "ts")

    if writer_kwargs.get("with_attributes"):
      encoder = None
    else:
      writer_kwargs["with_attributes"] = True
      _ = writer_kwargs.pop("timestamp_attribute", None)
      encoder = (
          EncodeToPubSubWithComputedAttributes(
              self.coder, attributes_fn, timestamp_attribute) if attributes_fn
          else EncodeToPubSubWithTimestamp(self.coder, timestamp_attribute))

    writer = self._writer_class(self.location, **writer_kwargs)

    do_fn = _DirectWriteToPubSubFn(writer._sink)
    do_fn.start_bundle()
    try:
      for element in elements:
        if encoder is None:
          message = element
        else:
          current_timestamp = Timestamp.from_utc_datetime(
              datetime.utcnow().replace(tzinfo=pytz.UTC))
          message = next(encoder.process(element, timestamp=current_timestamp))
        do_fn.process(message)
    finally:
      do_fn.finish_bundle()

  def truncate(self):
    sub_client = pubsub_v1.SubscriberClient()
    try:
      sub_client.delete_snapshot(self.snapshot_path)
      sub_client.delete_subscription(self.subscription_path)
    except gexc.NotFound:
      pass
    _ = sub_client.create_subscription(self.subscription_path, self.location)
    _ = sub_client.create_snapshot(self.snapshot_path, self.subscription_path)

  def remove(self):
    self._finalizer()

  @property
  def removed(self):
    return not self._finalizer.alive

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

  def _init_pubsub_resources(self):
    pub_client = pubsub_v1.PublisherClient()
    sub_client = pubsub_v1.SubscriberClient()

    try:
      _ = pub_client.create_topic(self.location)
    except gexc.AlreadyExists:
      if self._mode == "error":
        raise IOError("Topic '{}' already exists.".format(self.location))

    try:
      _ = sub_client.create_subscription(self.subscription_path, self.location)
    except gexc.AlreadyExists:
      if self._mode == "error":
        raise IOError("Subscription '{}' already exists.".format(
            self.subscription_path))
      if self._mode == "overwrite":
        sub_client.delete_subscription(self.subscription_path)
        _ = sub_client.create_subscription(self.subscription_path,
                                           self.location)

    if not self._single_use:
      try:
        _ = sub_client.create_snapshot(self.snapshot_path,
                                       self.subscription_path)
      except gexc.AlreadyExists:
        if self._mode == "error":
          raise IOError("Snapshot '{}' already exists.".format(
              self.snapshot_path))
        if self._mode == "overwrite":
          sub_client.delete_snapshot(self.snapshot_path)
          _ = sub_client.create_snapshot(self.snapshot_path,
                                         self.subscription_path)

  def _assert_topic_exists(self):
    pub_client = pubsub_v1.PublisherClient()
    try:
      _ = pub_client.get_topic(self.location)
    except gexc.NotFound:
      raise IOError("Pubsub topic '{}' does not exist.".format(self.location))

  def _assert_read_valid(self):
    if self._single_use and self._num_reads >= 1:
      raise ValueError(
          "A single-use cache allows only a single read over its lifetime.")

  def _get_or_create_subscription(self,
                                  subscription_path=None,
                                  snapshot_path=None):
    if subscription_path:
      return subscription_path
    elif self._single_use:
      return self.subscription_path
    else:
      subscription_path = self._create_new_subscription(self.location,
                                                        snapshot_path)
      return subscription_path

  def _create_new_subscription(self,
                               topic_path,
                               snapshot_path=None,
                               suffix=None):
    if suffix is None:
      suffix = "-{}".format(uuid.uuid4().hex)
    sub_client = pubsub_v1.SubscriberClient()
    subscription_path = (
        topic_path.replace("/topics/", "/subscriptions/") + suffix)
    _ = sub_client.create_subscription(subscription_path, topic_path)
    if snapshot_path is not None:
      # This operation appears to be especially flaky.
      sub_client.seek(
          subscription_path,
          snapshot=snapshot_path,
          retry=api_core.retry.Retry())
    return subscription_path


class PatchedPubSubReader(PTransform):

  def __init__(self, cache_class, reader_class, reader_args, reader_kwargs):
    self._cache_class = cache_class
    self._reader_class = reader_class
    self._reader_args = reader_args
    self._reader_kwargs = reader_kwargs

  def expand(self, pbegin):
    reader_kwargs = self._reader_kwargs.copy()
    coder = self._cache_class.coder

    if reader_kwargs.get("with_attributes"):
      reader = self._reader_class(*self._reader_args, **reader_kwargs)
      return pbegin | reader
    else:
      reader_kwargs["with_attributes"] = True
      if not reader_kwargs.get("timestamp_attribute"):
        reader_kwargs["timestamp_attribute"] = "ts"
      decoder = DecodeFromPubSub(coder, reader_kwargs["timestamp_attribute"])
      reader = self._reader_class(*self._reader_args, **reader_kwargs)
      return pbegin | reader | beam.ParDo(decoder)


class PatchedPubSubWriter(PTransform):

  def __init__(self, cache_class, writer_class, writer_args, writer_kwargs):
    self._cache_class = cache_class
    self._writer_class = writer_class
    self._writer_args = writer_args
    self._writer_kwargs = writer_kwargs

  def expand(self, pcoll):

    if self._cache_class.requires_coder and self._cache_class.coder is None:
      self._cache_class.coder = coders.registry.get_coder(pcoll.element_type)

    coder = self._cache_class.coder
    writer_kwargs = self._writer_kwargs.copy()
    attributes_fn = writer_kwargs.pop("attributes_fn", None)

    if writer_kwargs.get("with_attributes"):
      # The element is already encoded as a PubsubMessage
      if not writer_kwargs.get("timestamp_attribute"):
        raise ValueError("If the with_attributes option is set to True, "
                         "a timestamp_attribute must be provided.")
      writer = self._writer_class(*self._writer_args, **writer_kwargs)
      return pcoll | writer
    else:
      # Encode the element as a PubsubMesasage ourselves
      writer_kwargs["with_attributes"] = True
      # DirectRunner does not support timestamp_attribute, so we remove it
      # for every runner for consistency.
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
        attributes["ts"] = str(timestamp.micros)
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

    attributes = {self.timestamp_attribute: str(timestamp.micros)}
    element_bytes = self.coder.encode(element)
    message = PubsubMessage(element_bytes, attributes)
    yield message


def remove_topic_and_descendants(topic_path):
  project = beam_pubsub.parse_topic(topic_path)[0]
  project_path = pubsub_v1.PublisherClient.project_path(project)
  pub_client = pubsub_v1.PublisherClient()
  sub_client = pubsub_v1.SubscriberClient()
  try:
    snapshot_list = list(sub_client.list_snapshots(project_path))
  except gexc.MethodNotImplemented:
    snapshot_list = []
  # NB: Deletes could fail because of race conditions
  for snapshot in snapshot_list:
    if snapshot.topic == topic_path:
      try:
        sub_client.delete_snapshot(snapshot.name)
      except gexc.NotFound:
        pass
  try:
    subscription_list = list(pub_client.list_topic_subscriptions(topic_path))
  except gexc.NotFound:
    subscription_list = []
  for subscription in subscription_list:
    try:
      sub_client.delete_subscription(subscription)
    except gexc.NotFound:
      pass
  try:
    pub_client.delete_topic(topic_path)
  except gexc.NotFound:
    pass
