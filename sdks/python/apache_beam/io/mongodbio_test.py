import mock

from unittest import TestCase


class Test_BoundedMongoSource(TestCase):
  def test_client(self):
    self.fail()

  def test_estimate_size(self):
    self.fail()

  def test_split(self):
    self.fail()

  def test_get_range_tracker(self):
    self.fail()

  def test_read(self):
    self.fail()


class TestReadFromMongoDB(TestCase):
  @mock.patch('apache_beam.')
  def test_expand(self):
    self.fail()
