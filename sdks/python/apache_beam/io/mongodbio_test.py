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

import logging
import unittest

import mock

import apache_beam as beam
from apache_beam.io import ReadFromMongoDB
from apache_beam.io import WriteToMongoDB
from apache_beam.io.mongodbio import _BoundedMongoSource
from apache_beam.io.mongodbio import _GenerateObjectIdFn
from apache_beam.io.mongodbio import _MongoSink
from apache_beam.io.mongodbio import _WriteMongoFn
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class Test_BoundedMongoSource(unittest.TestCase):
  @mock.patch('apache_beam.io.mongodbio._BoundedMongoSource'
              '._get_document_count')
  @mock.patch('apache_beam.io.mongodbio._BoundedMongoSource'
              '._get_avg_document_size')
  def setUp(self, mock_size, mock_count):
    mock_size.return_value = 10
    mock_count.return_value = 30
    self.mongo_source = _BoundedMongoSource('mongodb://test', 'testdb',
                                            'testcoll')

  def test_estimate_size(self):
    self.assertEqual(self.mongo_source.estimate_size(), 300)

  def test_split(self):
    # desired bundle size is 3 times of avg doc size, each bundle contains 3
    # documents
    for bundle in self.mongo_source.split(start_position=0,
                                          stop_position=30,
                                          desired_bundle_size=30):
      self.assertEqual(3, bundle.weight)

    # expect 4 documents in first 7 bundles and 2 document in last bundle
    for bundle in self.mongo_source.split(40):
      self.assertIn(bundle.weight, [4, 2])

  @mock.patch('apache_beam.io.mongodbio.OffsetRangeTracker')
  def test_get_range_tracker(self, mock_tracker):
    self.mongo_source.get_range_tracker(None, None)
    mock_tracker.assert_called_with(0, 30)
    self.mongo_source.get_range_tracker(10, 20)
    mock_tracker.assert_called_with(10, 20)

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_read(self, mock_client):
    mock_tracker = mock.MagicMock()
    mock_tracker.try_claim.return_value = True
    mock_tracker.start_position.return_value = 0
    mock_tracker.stop_position.return_value = 2

    mock_client.return_value.__enter__.return_value.__getitem__.return_value \
      .__getitem__.return_value.find.return_value = [{'x': 1}, {'x': 2}]

    result = []
    for i in self.mongo_source.read(mock_tracker):
      result.append(i)
    self.assertListEqual([{'x': 1}, {'x': 2}], result)

  def test_display_data(self):
    data = self.mongo_source.display_data()
    self.assertTrue('uri' in data)
    self.assertTrue('database' in data)
    self.assertTrue('collection' in data)

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test__get_avg_document_size(self, mock_client):
    mock_client.return_value.__enter__.return_value.__getitem__ \
      .return_value.command.return_value = {'avgObjSize': 5}
    self.assertEqual(5, self.mongo_source._get_avg_document_size())

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_get_document_count(self, mock_client):
    mock_client.return_value.__enter__.return_value.__getitem__ \
      .return_value.__getitem__.return_value.count_documents.return_value = 10

    self.assertEqual(10, self.mongo_source._get_document_count())


class TestReadFromMongoDB(unittest.TestCase):
  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_read_from_mongodb(self, mock_client):
    objects = [{'x': 1}, {'x': 2}]
    mock_client.return_value.__enter__.return_value.__getitem__.return_value. \
      command.return_value = {'avgObjSize': 1}
    mock_client.return_value.__enter__.return_value.__getitem__.return_value. \
      __getitem__.return_value.find.return_value = objects
    mock_client.return_value.__enter__.return_value.__getitem__.return_value. \
      __getitem__.return_value.count_documents.return_value = 2

    with TestPipeline() as p:
      docs = p | 'ReadFromMongoDB' >> ReadFromMongoDB(
          uri='mongodb://test', db='db', coll='collection')
      assert_that(docs, equal_to(objects))


class Test_GenerateObjectIdFn(unittest.TestCase):
  def test_process(self):
    with TestPipeline() as p:
      output = (p | "Create" >> beam.Create([{
          'x': 1
      }, {
          'x': 2,
          '_id': 123
      }])
                | "Generate ID" >> beam.ParDo(_GenerateObjectIdFn())
                | "Check" >> beam.Map(lambda x: '_id' in x))
      assert_that(output, equal_to([True] * 2))


class Test_WriteMongoFn(unittest.TestCase):
  @mock.patch('apache_beam.io.mongodbio._MongoSink')
  def test_process(self, mock_sink):
    docs = [{'x': 1}, {'x': 2}, {'x': 3}]
    with TestPipeline() as p:
      _ = (p | "Create" >> beam.Create(docs)
           | "Write" >> beam.ParDo(_WriteMongoFn(batch_size=2)))
      p.run()

      self.assertEqual(
          2, mock_sink.return_value.__enter__.return_value.write.call_count)


class Test_MongoSink(unittest.TestCase):
  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_write(self, mock_client):
    docs = [{'x': 1}, {'x': 2}, {'x': 3}]
    _MongoSink(uri='test', db='test', coll='test').write(docs)
    self.assertTrue(mock_client.return_value.__getitem__.return_value.
                    __getitem__.return_value.bulk_write.called)


class TestWriteToMongoDB(unittest.TestCase):
  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_write_to_mongodb(self, mock_client):
    docs = [{'x': 1}, {'x': 2}, {'x': 3}]
    with TestPipeline() as p:
      _ = (p | "Create" >> beam.Create(docs)
           | "Write" >> WriteToMongoDB(db='test', coll='test'))
      p.run()
      self.assertTrue(mock_client.return_value.__getitem__.return_value.
                      __getitem__.return_value.bulk_write.called)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
