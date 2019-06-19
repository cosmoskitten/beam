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

import logging
import unittest

from hamcrest import all_of
from nose.plugins.attrib import attr
from pymongo import MongoClient

from apache_beam.io import mongodbio_pipeline
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline


class MongoDBIOIT(unittest.TestCase):
  def setUp(self):
    self.default_args = {
        'mongo_uri': 'mongodb://localhost:27017',
        'mongo_db': 'beam_mongodbio_it_test',
    }
    self.client = MongoClient(host=self.default_args['mongo_uri'])

  def tearDown(self):
    self.client.drop_database(self.default_args['mongo_db'])

  @attr('IT')
  def test_mongodbio_read_write(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    pipeline_verifiers = [
        PipelineStateMatcher(),
    ]
    mongodbio_pipeline.run(
        test_pipeline.get_full_options_as_args(
            on_success_matcher=all_of(*pipeline_verifiers),
            **self.default_args))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
