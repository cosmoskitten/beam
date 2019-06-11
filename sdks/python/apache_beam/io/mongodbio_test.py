from unittest import TestCase

import mock
import mongomock
import pymongo

from apache_beam.io import iobase

from apache_beam.testing.util import equal_to

from apache_beam.testing.util import assert_that

from apache_beam.io import ReadFromMongoDB
from apache_beam.testing.test_pipeline import TestPipeline

from apache_beam.io.mongodbio import _BoundedMongoSource


class Test_BoundedMongoSource(TestCase):
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
    for bundle in self.mongo_source.split(30):
      self.assertEqual(3, bundle.weight)

    # expect 4 documents in first 7 bundles and 2 document in last bundle
    for bundle in self.mongo_source.split(40):
      self.assertIn(bundle.weight, [4, 2])

  @mock.patch('apache_beam.io.mongodbio.OffsetRangeTracker')
  def test_get_range_tracker(self, mock_tracker):
    self.mongo_source.get_range_tracker(None, None)
    mock_tracker.assert_called_with(0, 30)

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_read(self, mock_client):
    mock_tracker = mock.MagicMock()
    mock_tracker.try_claim.return_value=True
    mock_tracker.start_position.return_value=0
    mock_tracker.stop_position.return_value=2

    mock_client.return_value.__enter__.return_value.__getitem__.return_value\
      .__getitem__.return_value.find.return_value = [{'x':1}, {'x':2}]

    result = []
    for i in self.mongo_source.read(mock_tracker):
      result.append(i)
    self.assertListEqual([{'x':1}, {'x':2}], result)


  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test__get_avg_document_size(self, mock_client):
    mock_client.return_value.__enter__.return_value.__getitem__\
      .return_value.command.return_value = {'avgObjSize': 5}
    self.assertEqual(5, self.mongo_source._get_avg_document_size())

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test__get_document_count(self, mock_client):
    mock_client.return_value.__enter__.return_value.__getitem__ \
      .return_value.__getitem__.return_value.count_documents.return_value = 10

    self.assertEqual(10, self.mongo_source._get_document_count())


class TestReadFromMongoDB(TestCase):
  @mongomock.patch()
  def test_read_from_mongodb(self):
    objects = [{'x':1}, {'x':2}, {'x':3}]
    client = pymongo.MongoClient('mongodb://localhost')
    client.db.collection.insert_many(objects)

    with TestPipeline() as p:
      docs = p | 'ReadFromMongoDB' >> \
             ReadFromMongoDB(uri='mongodb://localhost', db='db',
                             coll='collection')
      # assert_that(docs, equal_to([{'x':1}, {'x':2}, {'x':3}]))



