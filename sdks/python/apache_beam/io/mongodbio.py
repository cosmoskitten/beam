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
"""MongoDB IO

This module implements IO class to read and write data on MongoDB.


"""
from __future__ import absolute_import

from pymongo import MongoClient

from apache_beam.io import iobase
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.transforms import PTransform

__all__ = ['ReadFromMongoDB']


class _BoundedMongoSource(iobase.BoundedSource):
  def __init__(self,
               uri=None,
               db=None,
               coll=None,
               filter=None,
               projection=None,
               num_splits=0,
               **kwargs):
    if filter is None:
      filter = {}
    if num_splits < 0:
      raise ValueError("number of splits should not be negative")
    self._uri = uri
    self._db = db
    self._coll = coll
    self._filter = filter
    self._projection = projection
    self._num_splits = num_splits
    self._client = None
    self._spec = kwargs

  @property
  def client(self):
    if self._client is None:
      self._client = MongoClient(host=self._uri, kwargs=self._spec)
    return self._client

  def estimate_size(self):
    return self.client[self._db][self._coll].count_documents(self._filter)

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    if start_position is None:
      start_position = 0
    if stop_position is None:
      stop_position = self.estimate_size()

    desired_bundle_size = self.estimate_size() // (self._num_splits + 1)
    bundle_start = start_position
    while bundle_start < stop_position:
      bundle_end = max(stop_position, bundle_start + desired_bundle_size)
      yield iobase.SourceBundle(weight=bundle_end - bundle_start,
                                source=self,
                                start_position=bundle_start,
                                stop_position=bundle_end)
      bundle_start = bundle_end

  def get_range_tracker(self, start_position, stop_position):
    if start_position is None:
      start_position = 0
    if stop_position is None:
      stop_position = self.estimate_size()
    return OffsetRangeTracker(start_position, stop_position)

  def read(self, range_tracker):
    if range_tracker.try_claim(range_tracker.start_position()):
      docs = self.client[self._db][self._coll].find(
          filter=self._filter, projection=self._projection
      )[range_tracker.start_position():range_tracker.stop_position()]
      for doc in docs:
        yield doc


class ReadFromMongoDB(PTransform):
  def __init__(self,
               uri='mongodb://localhost:27017',
               db=None,
               coll=None,
               filter=None,
               projection=None,
               num_splits=0,
               **spec):
    self._mongo_source = _BoundedMongoSource(uri=uri,
                                             db=db,
                                             coll=coll,
                                             filter=filter,
                                             projection=projection,
                                             num_splits=num_splits,
                                             spec=spec)

  def expand(self, pcoll):
    return pcoll | iobase.Read(self._mongo_source)

