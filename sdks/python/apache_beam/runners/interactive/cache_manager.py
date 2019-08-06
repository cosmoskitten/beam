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
from __future__ import division
from __future__ import print_function

import uuid
from collections import MutableMapping
from datetime import datetime

import apache_beam as beam
from apache_beam.io import filesystems
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.interactive.caching import filebasedcache
from apache_beam.runners.interactive.caching import pcollectioncache
from apache_beam.runners.interactive.caching import streambasedcache
from apache_beam.transforms import combiners


def _terminal_subclasses(cls):
  terminal_subclasses = []

  for subclass in cls.__subclasses__():
    if subclass.__subclasses__():
      terminal_subclasses.extend(_terminal_subclasses(subclass))
    else:
      terminal_subclasses.append(subclass)

  return terminal_subclasses


class CacheManager(MutableMapping):

  _cache_classes = {
      cls.__name__: cls
      for cls in _terminal_subclasses(pcollectioncache.PCollectionCache)
  }

  def __init__(self, pipeline_options):
    self._options = pipeline_options
    self._data = {}

  def __delitem__(self, key):
    del self._data[key]

  def __getitem__(self, key):
    return self._data[key]

  def __iter__(self):
    return iter(self._data)

  def __len__(self):
    return len(self._data)

  def __setitem__(self, key, value):
    self._data[key] = value

  @staticmethod
  def create_cache(cache_class, cache_location, **writer_kwargs):
    for _ in range(3):
      unique_cache_location = "{}-{}-{}".format(
          cache_location,
          datetime.now().strftime("%Y-%m-%d-%H%M%S"),
          uuid.uuid4().hex[:8])
      try:
        return cache_class(unique_cache_location, **writer_kwargs)
      except IOError as e:
        pass
    raise e

  def create_default_cache(self,
                           cache_name="cache",
                           cache_class=None,
                           **writer_kwargs):
    cache_location_root = self._get_default_cache_location()
    # Prefix with 'cache-' because in some cases (e.g. PubSub) the name can't
    # start with a number.
    cache_location = cache_location_root + cache_name

    if isinstance(cache_class, str):
      cache_class = self._cache_classes[cache_class]
    elif cache_class is None:
      cache_class = self._get_default_cache_class()

    cache = self.create_cache(cache_class, cache_location, **writer_kwargs)
    return cache

  def _get_default_cache_location(self):
    standard_options = self._options.view_as(StandardOptions)
    google_cloud_options = self._options.view_as(GoogleCloudOptions)
    if standard_options.streaming:
      from google.cloud import pubsub_v1
      cache_location_root = pubsub_v1.PublisherClient.topic_path(
          google_cloud_options.project, "")
    else:
      cache_location_root = filesystems.FileSystems.join(
          google_cloud_options.temp_location, "cache", "")
    return cache_location_root

  def _get_default_cache_class(self):
    standard_options = self._options.view_as(StandardOptions)
    if standard_options.streaming:
      return streambasedcache.PubSubBasedCache
    else:
      return filebasedcache.SafeTextBasedCache

  def cleanup(self):
    for key in list(self):
      cache = self.pop(key)
      cache.remove()


class ReadCache(beam.PTransform):
  """A PTransform that reads the PCollections from the cache."""

  def __init__(self, cache_manager, label, **reader_kwargs):
    self._cache_manager = cache_manager
    self._label = label
    self._reader_kwargs = reader_kwargs

  def expand(self, pbegin):
    cache = self._cache_manager[("full", self._label)]
    # pylint: disable=expression-not-assigned
    return pbegin | 'Read' >> cache.reader()


class WriteCache(beam.PTransform):
  """A PTransform that writes the PCollections to the cache."""

  def __init__(self,
               cache_manager,
               label,
               sample=None,
               cache_class=None,
               **writer_kwargs):
    self._cache_manager = cache_manager
    self._label = label
    self._sample = sample
    self._cache_class = cache_class
    self._writer_kwargs = writer_kwargs

  def expand(self, pcoll):
    cache_type = 'sample' if self._sample is not None else 'full'
    cache_name = "cache-{}-{}".format(self._label, cache_type)
    cache = self._cache_manager.create_default_cache(cache_name,
                                                     self._cache_class,
                                                     **self._writer_kwargs)
    self._cache_manager[(cache_type, self._label)] = cache

    if self._sample:
      pcoll |= 'Sample' >> (
          combiners.Sample.FixedSizeGlobally(self._sample)
          | beam.FlatMap(lambda sample: sample))
    # pylint: disable=expression-not-assigned
    return pcoll | 'Write' >> cache.writer()
