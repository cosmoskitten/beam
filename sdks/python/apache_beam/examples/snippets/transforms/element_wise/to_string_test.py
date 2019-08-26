# coding=utf-8
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

import mock

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from . import to_string


def check_plants(actual):
  # [START plants]
  plants = [
      '🍓,Strawberry',
      '🥕,Carrot',
      '🍆,Eggplant',
      '🍅,Tomato',
      '🥔,Potato',
  ]
  # [END plants]
  assert_that(actual, equal_to(plants))


def check_plant_objects(actual):
  # [START plant_objects]
  plants_objects = [
      "{'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'}",
      "{'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'}",
      "{'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'}",
      "{'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'}",
      "{'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'}",
  ]
  # [END plants_objects]
  assert_that(actual, equal_to(plants_objects))


def check_plants_csv(actual):
  # [START plant_objects]
  plants_objects = [
      '🍓,Strawberry,perennial',
      '🥕,Carrot,biennial',
      '🍆,Eggplant,perennial',
      '🍅,Tomato,annual',
      '🥔,Potato,perennial',
  ]
  # [END plants_csv]
  assert_that(actual, equal_to(plants_objects))


@mock.patch('apache_beam.Pipeline', TestPipeline)
# pylint: disable=line-too-long
@mock.patch('apache_beam.examples.snippets.transforms.element_wise.to_string.print', lambda elem: elem)
# pylint: enable=line-too-long
class ToStringTest(unittest.TestCase):
  def test_to_string_kvs(self):
    to_string.to_string_kvs(check_plants)

  def test_to_string_element(self):
    to_string.to_string_element(check_plant_objects)

  def test_to_string_iterables(self):
    to_string.to_string_iterables(check_plants_csv)


if __name__ == '__main__':
  unittest.main()
