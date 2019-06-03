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

"""
A pipeline that reads data from a BigQuery table and counts the number of
rows.

Can be configured to simulate slow reading for a given number of rows.
"""

from __future__ import absolute_import

import random
import time

from apache_beam import DoFn
from apache_beam import ParDo
from apache_beam.io import BigQuerySource
from apache_beam.io import Read
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.combiners import Count


class RowToStringWithSlowDown(DoFn):

  def process(self, element, num_slow=0, *args, **kwargs):

    if num_slow == 0:
      yield ['row']
    else:
      rand = random.random() * 100
      if rand < num_slow:
        time.sleep(0.01)
        yield ['slow_row']
      else:
        yield ['row']


def run_pipeline(pipeline, input_dataset, input_table, num_slow, num_records):
  p = (pipeline
       | 'Read from BigQuery' >> Read(BigQuerySource(dataset=input_dataset,
                                                     table=input_table))
       | 'Row to string' >> ParDo(RowToStringWithSlowDown(),
                                  num_slow=num_slow)
       | 'Count' >> Count.Globally())

  pipeline.run()
  assert_that(p, equal_to([num_records]))
