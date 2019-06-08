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

from apache_beam.testing.test_pipeline import TestPipeline
from bson import objectid
from pymongo import MongoClient
from pymongo import ReplaceOne

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.transforms import DoFn
from apache_beam.transforms import PTransform
from apache_beam.transforms import Reshuffle

__all__ = ['ReadFromMongoDB', 'WriteToMongoDB']


class _BoundedMongoSource(iobase.BoundedSource):
  def __init__(self,
               uri=None,
               db=None,
               coll=None,
               filter=None,
               projection=None,
               **kwargs):
    if filter is None:
      filter = {}
    self._uri = uri
    self._db = db
    self._coll = coll
    self._filter = filter
    self._projection = projection
    # self._client = MongoClient(host=uri, **kwargs)
    # self._avg_size = self.client[self._db].command('collstats',
    #                                                self._coll)['avgObjSize']
    # self._count = self.client[self._db][self._coll].count_documents(
    #     self._filter)


  def estimate_size(self):
    with MongoClient(self._uri) as client:
      avg_size = client[self._db].command('collstats', self._coll)['avgObjSize']
      count = client[self._db][self._coll].count_documents(self._filter)
      return avg_size * count

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    # use document index as the start and stop position
    if start_position is None:
      start_position = 0
    if stop_position is None:
      stop_position = self._count

    # get an estimate on how many documents should be included in a split batch
    # desired_bundle_count = desired_bundle_size // self._avg_size
    desired_bundle_count = desired_bundle_size // 10

    bundle_start = start_position
    while bundle_start < stop_position:
      bundle_end = max(stop_position, bundle_start + desired_bundle_count)
      yield iobase.SourceBundle(weight=bundle_end - bundle_start,
                                source=self,
                                start_position=bundle_start,
                                stop_position=bundle_end)
      bundle_start = bundle_end

  def get_range_tracker(self, start_position, stop_position):
    if start_position is None:
      start_position = 0
    if stop_position is None:
      # stop_position = self._count
      stop_position = 39
    return OffsetRangeTracker(start_position, stop_position)

  def read(self, range_tracker):
    print('trying to read')
    if range_tracker.try_claim(range_tracker.start_position()):
      with MongoClient(self._uri) as client:
        docs = client[self._db][self._coll].find(
            filter=self._filter, projection=self._projection
        )[range_tracker.start_position():range_tracker.stop_position()]
        for doc in docs:
          print(doc)
          yield doc


class ReadFromMongoDB(PTransform):
  def __init__(self,
               uri='mongodb://localhost:27017',
               db=None,
               coll=None,
               filter=None,
               projection=None,
               **kwargs):
    self._mongo_source = _BoundedMongoSource(uri=uri,
                                             db=db,
                                             coll=coll,
                                             filter=filter,
                                             projection=projection,
                                             **kwargs)

  def expand(self, pcoll):
    return pcoll | iobase.Read(self._mongo_source)


class WriteToMongoDB(PTransform):
  def __init__(self,
               uri='mongodb://localhost:27017',
               db=None,
               coll=None,
               batch_size=1,
               reserve_id=False,
               **kwargs):
    self._uri = uri
    self._db = db
    self._coll = coll
    self._batch_size = batch_size
    self._reserve_id = reserve_id
    self._spec = kwargs

  def expand(self, pcoll):
    return pcoll \
           | beam.ParDo(_GenerateObjectIdFn(self._reserve_id)) \
           | Reshuffle() \
           | beam.ParDo(_WriteMongoFn(self._uri, self._db, self._coll,
                                      self._batch_size, **self._spec))


class _GenerateObjectIdFn(DoFn):
  def __init__(self, reserve_id=False):
    self._reserve_id = reserve_id

  def process(self, element, *args, **kwargs):
    if not self._reserve_id or '_id' not in element:
      result = element.copy()
      result.update('_id', objectid.ObjectId())
      return result
    return element


class _WriteMongoFn(DoFn):
  def __init__(self, uri=None, db=None, coll=None, batch_size=1, **kwargs):
    self._uri = uri
    self._db = db
    self._coll = coll
    self._batch_size = batch_size
    self._spec = kwargs
    self._mongo_sink = None
    self._batch = None

  def setup(self):
    self._mongo_sink = _MongoSink(self._uri, self._db, self._coll, **self._spec)

  def start_bundle(self):
    self._batch = []

  def finish_bundle(self):
    self._flush()

  def teardown(self):
    self._mongo_sink.close()

  def process(self, element, *args, **kwargs):
    self._batch.append(element)
    if len(self._batch) >= self._batch_size:
      self._flush()

  def _flush(self):
    if len(self._batch) == 0:
      return
    self._mongo_sink.write(self._batch)
    self._batch.clear()


class _MongoSink(object):
  def __init__(self, uri=None, db=None, coll=None, **kwargs):
    self._uri = uri
    self._db = db
    self._coll = coll
    self._client = None
    self._spec = kwargs

  @property
  def client(self):
    if self.client is None:
      self._client = MongoClient(host=self._uri, **self._spec)
    return self._client

  def write(self, documents):
    requests = []
    for doc in documents:
      requests.append(
          ReplaceOne(filter={'_id', doc['_id']}, replacement=doc, upsert=True))
    self.client[self._db][self._coll].bulk_write(requests)

  def close(self):
    self.client.close()


if __name__ == '__main__':
  pipeline = TestPipeline()


  mongo_source = _BoundedMongoSource(uri='mongodb://localhost:27017', db='test', coll='testData')
  pipeline | iobase.Read(mongo_source)
  res = pipeline.run()
  res.wait_until_finish()

