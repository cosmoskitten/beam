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
"""This module implements IO classes to read and write data on MongoDB.


Read from MongoDB
-----------------
:class:`ReadFromMongoDB` is a ``PTransform`` that reads from configured MongoDB
source and returns ``PCollection`` of dict representing MongoDB document.
To configure MongoDB source, the URI, database name, collection name needs to be
provided.

Example usage::

  pipeline | ReadFromMongoDB(uri='mongodb://localhost:27017',
                             db='testdb',
                             coll='input')


Write to MongoDB:
-----------------
:class:`WriteToMongoDB` is a ``PTransform`` that writes MongoDB documents to
configured sink, and the write is conducted through a mongodb bulk_write of
``ReplaceOne`` operations. If the document's _id field already existed in the
MongoDB collection, it results in an overwrite, otherwise, a new document
will be inserted.

Example usage::

  pipeline | WriteToMongoDB(uri='mongodb://localhost:27017',
                            db='testdb',
                            coll='output',
                            batch_size=10)


No backward compatibility guarantees. Everything in this module is experimental.
"""

from __future__ import absolute_import

from bson import objectid
from pymongo import MongoClient
from pymongo import ReplaceOne

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.transforms import DoFn
from apache_beam.transforms import PTransform
from apache_beam.transforms import Reshuffle
from apache_beam.utils.annotations import experimental

__all__ = ['ReadFromMongoDB', 'WriteToMongoDB']


@experimental()
class ReadFromMongoDB(PTransform):
  """A ``PTransfrom`` to read MongoDB documents into a ``PCollection``.
  """

  def __init__(self,
               uri='mongodb://localhost:27017',
               db=None,
               coll=None,
               filter=None,
               projection=None,
               **kwargs):
    """Initialize a :class:`ReadFromMongoDB`

    Args:
      uri (str): The MongoDB connection string following the URI format
      db (str): The MongoDB database name
      coll (str): The MongoDB collection name
      filter: A :class:`~bson.SON` object specifying elements
        which must be present for a document to be included in the result set
      projection (list): A list of field names that should be
        returned in the result set or a dict specifying the fields to include or
        exclude
      **kwargs: Optional :class:`~pymongo.mongo_client.MongoClient`
        parameters as keyword arguments

    Returns:
      :class:`~apache_beam.transforms.PTransform`

    """
    self._mongo_source = _BoundedMongoSource(uri=uri,
                                             db=db,
                                             coll=coll,
                                             filter=filter,
                                             projection=projection,
                                             **kwargs)

  def expand(self, pcoll):
    return pcoll | iobase.Read(self._mongo_source)


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
    self._spec = kwargs
    self._doc_count = self._get_document_count()
    self._avg_doc_size = self._get_avg_document_size()

  def estimate_size(self):
    return self._avg_doc_size * self._doc_count

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    # use document cursor index as the start and stop positions
    if start_position is None:
      start_position = 0
    if stop_position is None:
      stop_position = self._doc_count

    # get an estimate on how many documents should be included in a split batch
    desired_bundle_count = desired_bundle_size // self._avg_doc_size

    bundle_start = start_position
    while bundle_start < stop_position:
      bundle_end = min(stop_position, bundle_start + desired_bundle_count)
      yield iobase.SourceBundle(weight=bundle_end - bundle_start,
                                source=self,
                                start_position=bundle_start,
                                stop_position=bundle_end)
      bundle_start = bundle_end

  def get_range_tracker(self, start_position, stop_position):
    if start_position is None:
      start_position = 0
    if stop_position is None:
      stop_position = self._doc_count
    return OffsetRangeTracker(start_position, stop_position)

  def read(self, range_tracker):
    if range_tracker.try_claim(range_tracker.start_position()):
      with MongoClient(self._uri, **self._spec) as client:
        docs = client[self._db][self._coll].find(
            filter=self._filter, projection=self._projection
        )[range_tracker.start_position():range_tracker.stop_position()]
        for doc in docs:
          yield doc

  def _get_avg_document_size(self):
    with MongoClient(self._uri, **self._spec) as client:
      return client[self._db].command('collstats', self._coll)['avgObjSize']

  def _get_document_count(self):
    with MongoClient(self._uri, **self._spec) as client:
      return client[self._db][self._coll].count_documents(self._filter)


@experimental()
class WriteToMongoDB(PTransform):
  def __init__(self,
               uri='mongodb://localhost:27017',
               db=None,
               coll=None,
               batch_size=1,
               **kwargs):
    """

    Args:
      uri (str): The MongoDB connection string following the URI format
      db (str): The MongoDB database name
      coll (str): The MongoDB collection name
      batch_size(int): Number of documents per bulk_write to  MongoDB
      **kwargs: Optional :class:`~pymongo.mongo_client.MongoClient`
        parameters as keyword arguments
    """
    self._uri = uri
    self._db = db
    self._coll = coll
    self._batch_size = batch_size
    self._spec = kwargs

  def expand(self, pcoll):
    return pcoll \
           | beam.ParDo(_GenerateObjectIdFn(self._reserve_id)) \
           | Reshuffle() \
           | beam.ParDo(_WriteMongoFn(self._uri, self._db, self._coll,
                                      self._batch_size, **self._spec))


class _GenerateObjectIdFn(DoFn):
  def process(self, element, *args, **kwargs):
    # if _id field already exist we keep it as it is, otherwise the ptransform
    # generates a new _id field to achieve idempotent write to mongodb.
    if '_id' not in element:
      result = element.copy()
      result['_id'] = objectid.ObjectId()
      yield result
    yield element


class _WriteMongoFn(DoFn):
  def __init__(self, uri=None, db=None, coll=None, batch_size=1, **kwargs):
    self._batch_size = batch_size
    self._spec = kwargs
    self._mongo_sink = _MongoSink(uri, db, coll, **self._spec)
    self._batch = None

  def start_bundle(self):
    self._batch = []

  def finish_bundle(self):
    self._flush()

  def process(self, element, *args, **kwargs):
    self._batch.append(element)
    if len(self._batch) >= self._batch_size:
      self._flush()

  def _flush(self):
    if len(self._batch) == 0:
      return
    self._mongo_sink.write(self._batch)
    self._batch = []


class _MongoSink(object):
  def __init__(self, uri=None, db=None, coll=None, **kwargs):
    self._uri = uri
    self._db = db
    self._coll = coll
    self._spec = kwargs

  def write(self, documents):
    requests = []
    for doc in documents:
      # match document based on _id field, if not found in current collection,
      # insert new one, otherwise overwrite it.
      requests.append(
          ReplaceOne(filter={'_id': doc['_id']}, replacement=doc, upsert=True))
    with MongoClient(self._uri, **self._spec) as client:
      client[self._db][self._coll].bulk_write(requests)
