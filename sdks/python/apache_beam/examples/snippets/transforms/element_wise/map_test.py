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

from apache_beam.examples.snippets.transforms.element_wise.map import *
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch('apache_beam.examples.snippets.transforms.element_wise.map.print', lambda elem: elem)
class MapTest(unittest.TestCase):
  def __init__(self, methodName):
    super().__init__(methodName)
    # [START plants]
    plants = [
        'Strawberry',
        'Carrot',
        'Artichoke',
        'Tomato',
        'Potato',
    ]
    # [END plants]
    self.plants_test = lambda actual: assert_that(actual, equal_to(plants))

    # [START plant_details]
    plant_details = [
        {'name': 'Strawberry', 'duration': 'perennial'},
        {'name': 'Carrot', 'duration': 'biennial'},
        {'name': 'Artichoke', 'duration': 'perennial'},
        {'name': 'Tomato', 'duration': 'annual'},
        {'name': 'Potato', 'duration': 'perennial'},
    ]
    # [END plant_details]
    self.plant_details_test = lambda actual: \
        assert_that(actual, equal_to(plant_details))

  def test_map_simple(self):
    map_simple(self.plants_test)

  def test_map_function(self):
    map_function(self.plants_test)

  def test_map_lambda(self):
    map_lambda(self.plants_test)

  def test_map_multiple_arguments(self):
    map_multiple_arguments(self.plants_test)

  def test_map_side_inputs_singleton(self):
    map_side_inputs_singleton(self.plants_test)

  def test_map_side_inputs_iter(self):
    map_side_inputs_iter(self.plants_test)

  def test_map_side_inputs_dict(self):
    map_side_inputs_dict(self.plant_details_test)


if __name__ == '__main__':
  unittest.main()
