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
"""Unit tests for the apiclient module."""

import unittest

from apache_beam.internal import apiclient
from apache_beam.io import iobase

import apache_beam.internal.clients.dataflow as dataflow


class UtilTest(unittest.TestCase):

  def test_reader_progress_to_cloud_progress_position(self):
    reader_position = iobase.ReaderPosition(byte_offset=9999)
    reader_progress = iobase.ReaderProgress(position=reader_position)

    cloud_progress = apiclient.reader_progress_to_cloud_progress(
        reader_progress)
    self.assertIsNotNone(cloud_progress)
    self.assertIsInstance(cloud_progress, dataflow.ApproximateProgress)
    self.assertIsNotNone(cloud_progress.position)
    self.assertIsInstance(cloud_progress.position, dataflow.Position)
    self.assertEquals(9999, cloud_progress.position.byteOffset)

  def test_reader_progress_to_cloud_progress_percent_complete(self):
    reader_progress = iobase.ReaderProgress(percent_complete=0.123)

    cloud_progress = apiclient.reader_progress_to_cloud_progress(
        reader_progress)
    self.assertIsNotNone(cloud_progress)
    self.assertIsInstance(cloud_progress, dataflow.ApproximateProgress)
    self.assertIsNotNone(cloud_progress.percentComplete)
    self.assertEquals(0.123, cloud_progress.percentComplete)

  def test_reader_position_to_cloud_position(self):
    reader_position = iobase.ReaderPosition(byte_offset=9999)

    cloud_position = apiclient.reader_position_to_cloud_position(
        reader_position)
    self.assertIsNotNone(cloud_position)

  def test_dynamic_split_result_with_position_to_cloud_stop_position(self):
    position = iobase.ReaderPosition(byte_offset=9999)
    dynamic_split_result = iobase.DynamicSplitResultWithPosition(position)

    approximate_position = (
        apiclient.dynamic_split_result_with_position_to_cloud_stop_position(
            dynamic_split_result))
    self.assertIsNotNone(approximate_position)
    self.assertIsInstance(approximate_position, dataflow.Position)
    self.assertEqual(9999, approximate_position.byteOffset)

  def test_cloud_progress_to_reader_progress_index_position(self):
    cloud_progress = dataflow.ApproximateProgress()
    cloud_progress.position = dataflow.Position()
    cloud_progress.position.byteOffset = 9999

    reader_progress = apiclient.cloud_progress_to_reader_progress(
        cloud_progress)
    self.assertIsNotNone(reader_progress.position)
    self.assertIsInstance(reader_progress.position, iobase.ReaderPosition)
    self.assertEqual(9999, reader_progress.position.byte_offset)

  def test_cloud_progress_to_reader_progress_percent_complete(self):
    cloud_progress = dataflow.ApproximateProgress()
    cloud_progress.percentComplete = 0.123

    reader_progress = apiclient.cloud_progress_to_reader_progress(
        cloud_progress)
    self.assertIsNotNone(reader_progress.percent_complete)
    self.assertEqual(0.123, reader_progress.percent_complete)

  def test_cloud_position_to_reader_position_byte_offset(self):
    cloud_position = dataflow.Position()
    cloud_position.byteOffset = 9999

    reader_position = apiclient.cloud_position_to_reader_position(
        cloud_position)
    self.assertIsNotNone(reader_position)
    self.assertIsInstance(reader_position, iobase.ReaderPosition)
    self.assertEqual(9999, reader_position.byte_offset)

  def test_approximate_progress_to_dynamic_split_request(self):
    approximate_progress = dataflow.ApproximateProgress()
    approximate_progress.percentComplete = 0.123

    dynamic_split_request = (
        apiclient.approximate_progress_to_dynamic_split_request(
            approximate_progress))
    self.assertIsNotNone(dynamic_split_request)
    self.assertIsInstance(dynamic_split_request.progress, iobase.ReaderProgress)
    self.assertIsNotNone(dynamic_split_request.progress.percent_complete)
    self.assertEqual(dynamic_split_request.progress.percent_complete, 0.123)

  def test_bounded_source_split_to_cloud_dynamic_source_split(self):
    primary = iobase.SourceBundle(weight=0.2, source=iobase.BoundedSource(),
                                  start_position=100, stop_position=200)
    residual = iobase.SourceBundle(weight=0.8, source=iobase.BoundedSource(),
                                   start_position=200, stop_position=300)
    bounded_source_split = iobase.BoundedSourceSplit(primary=primary,
                                                     residual=residual)

    cloud_split = apiclient.bounded_source_split_to_cloud_dynamic_source_split(
        bounded_source_split)

    self.assertTrue(
        isinstance(cloud_split, dataflow.DynamicSourceSplit))
    self.assertTrue(
        isinstance(cloud_split.primary, dataflow.DerivedSource))
    self.assertTrue(
        isinstance(cloud_split.primary.source, dataflow.Source))

    self.assertTrue(
        isinstance(cloud_split.residual, dataflow.DerivedSource))
    self.assertTrue(
        isinstance(cloud_split.residual.source, dataflow.Source))

  def test_splits_to_split_response(self):
    bundles = []
    bundles.append(
        iobase.SourceBundle(weight=0.2, source=iobase.BoundedSource(),
                            start_position=100, stop_position=200))
    bundles.append(
        iobase.SourceBundle(weight=0.5, source=iobase.BoundedSource(),
                            start_position=200, stop_position=300))
    bundles.append(
        iobase.SourceBundle(weight=0.3, source=iobase.BoundedSource(),
                            start_position=300, stop_position=400))

    split_response = apiclient.splits_to_split_response(bundles)

    self.assertTrue(
        isinstance(split_response, dataflow.SourceOperationResponse))
    self.assertEqual(3, len(split_response.split.bundles))


if __name__ == '__main__':
  unittest.main()
