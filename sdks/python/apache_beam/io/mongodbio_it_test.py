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


class MongoDBIOIntegrationTest(unittest.TestCase):
  @attr('IT')
  def test_mongodbio_read_write(self):
    default_args = {
        'mongo_uri': 'mongodb://localhost:27017',
        'mongo_db': 'beam_mongodbio_it_test',
    }
    test_pipeline = TestPipeline(is_integration_test=True)
    pipeline_verifiers = [
        PipelineStateMatcher(),
    ]
    mongodbio_pipeline.run(
        test_pipeline.get_full_options_as_args(
            on_success_matcher=all_of(*pipeline_verifiers), **default_args))

    # clean up
    with MongoClient(default_args['mongo_db']) as client:
      client.drop_database(default_args['mongo_db'])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
