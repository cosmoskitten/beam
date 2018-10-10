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

import logging
import random
import unittest
from datetime import datetime

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.io.gcp import datastore_write
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline


class DatastoreWriteIT(unittest.TestCase):

  NUM_ENTITIES_DIRECT = 1001
  NUM_ENTITIES_SERVICE = 30000

  def run_datastore_write(self, num_entities, limit=None):
    test_pipeline = TestPipeline(is_integration_test=True)
    current_time = datetime.now().strftime("%m%d%H%M%S")
    seed = random.randint(0, 100000)
    kind = 'testkind%s%d' % (current_time, seed)
    pipeline_verifiers = [PipelineStateMatcher()]
    extra_opts = {'kind': kind,
                  'num_entities': num_entities,
                  'on_success_matcher': all_of(*pipeline_verifiers)}
    if limit is not None:
      extra_opts['--limit'] = limit

    datastore_write.run(test_pipeline.get_full_options_as_args(
        **extra_opts))

  @attr('IT')
  def test_datastore_write(self):
    self.run_datastore_write(self.NUM_ENTITIES_DIRECT)

  @attr('IT')
  def test_datastore_write_limit(self):
    self.run_datastore_write(self.NUM_ENTITIES_SERVICE, limit=1000)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
