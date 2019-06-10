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
from __future__ import print_function

import unittest
from unittest import mock

from apache_beam.examples.snippets.transforms.element_wise.partition import *
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch('apache_beam.examples.snippets.transforms.element_wise.partition.print', lambda elem: elem)
class PartitionTest(unittest.TestCase):
  def __init__(self, methodName):
    super().__init__(methodName)
    # [START partitions]
    annuals = [
        {'name': 'Tomato', 'duration': 'annual'},
    ]
    biennials = [
        {'name': 'Carrot', 'duration': 'biennial'},
    ]
    perennials = [
        {'name': 'Strawberry', 'duration': 'perennial'},
        {'name': 'Artichoke', 'duration': 'perennial'},
        {'name': 'Potato', 'duration': 'perennial'},
    ]
    # [END partitions]
    def partitions_test(actual1, actual2, actual3):
      assert_that(actual1, equal_to(annuals), label='assert annuals')
      assert_that(actual2, equal_to(biennials), label='assert biennials')
      assert_that(actual3, equal_to(perennials), label='assert perennials')
    self.partitions_test = partitions_test

    # [START train_test]
    train_dataset = [
        {'name': 'Carrot', 'duration': 'biennial'},
        {'name': 'Artichoke', 'duration': 'perennial'},
        {'name': 'Tomato', 'duration': 'annual'},
        {'name': 'Potato', 'duration': 'perennial'},
    ]
    test_dataset = [
        {'name': 'Strawberry', 'duration': 'perennial'},
    ]
    # [END train_test]
    def train_test_split_test(actual1, actual2):
      assert_that(actual1, equal_to(train_dataset), label='assert train')
      assert_that(actual2, equal_to(test_dataset), label='assert test')
    self.train_test_split_test = train_test_split_test

  def test_partition_function(self):
    partition_function(self.partitions_test)

  def test_partition_lambda(self):
    partition_lambda(self.partitions_test)

  def test_partition_multiple_arguments(self):
    partition_multiple_arguments(self.train_test_split_test)

  # def test_partition_side_inputs_singleton(self):
  #   partition_side_inputs_singleton(self.plants_test)

  # def test_partition_side_inputs_iter(self):
  #   partition_side_inputs_iter(self.plants_test)

  # def test_partition_side_inputs_dict(self):
  #   partition_side_inputs_dict(self.plant_details_test)


if __name__ == '__main__':
  unittest.main()
