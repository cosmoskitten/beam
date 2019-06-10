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

from apache_beam.examples.snippets.transforms.element_wise.pardo import *
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


@mock.patch('apache_beam.Pipeline', TestPipeline)
# pylint: disable=line-too-long
@mock.patch('apache_beam.examples.snippets.transforms.element_wise.pardo.print', lambda elem: elem)
# pylint: enable=line-too-long
class ParDoTest(unittest.TestCase):
  def __init__(self, methodName):
    super(ParDoTest, self).__init__(methodName)
    # [START plants]
    plants = [
        '🍓Strawberry',
        '🥕Carrot',
        '🍆Eggplant',
        '🍅Tomato',
        '🥔Potato',
    ]
    # [END plants]
    self.plants_test = lambda actual: assert_that(actual, equal_to(plants))

    # [START valid_plants]
    valid_plants = [
        {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'},
        {'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'},
        {'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'},
        {'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'},
    ]
    # [END valid_plants]
    self.valid_plants_test = lambda actual: \
        assert_that(actual, equal_to(valid_plants))

    # pylint: disable=line-too-long
    # [START dofn_params]
    dofn_params = '''\
# timestamp
type(timestamp) -> <class 'apache_beam.utils.timestamp.Timestamp'>
timestamp.micros -> 1584675660000000
timestamp.to_rfc3339() -> '2020-03-20T03:41:00Z'
timestamp.to_utc_datetime() -> datetime.datetime(2020, 3, 20, 3, 41)

# window
type(window) -> <class 'apache_beam.transforms.window.IntervalWindow'>
window.start -> Timestamp(1584675660) (2020-03-20 03:41:00)
window.end -> Timestamp(1584675690) (2020-03-20 03:41:30)
window.max_timestamp() -> Timestamp(1584675689.999999) (2020-03-20 03:41:29.999999)'''
    # [END dofn_params]
    # pylint: enable=line-too-long
    self.dofn_params_test = lambda actual: \
        assert_that(actual, equal_to([dofn_params]))

  def test_pardo_simple(self):
    pardo_simple(self.plants_test)

  def test_pardo_function(self):
    pardo_function(self.plants_test)

  def test_pardo_lambda(self):
    pardo_lambda(self.plants_test)

  def test_pardo_generator(self):
    pardo_generator(self.plants_test)

  def test_pardo_multiple_arguments(self):
    pardo_multiple_arguments(self.plants_test)

  def test_pardo_dofn(self):
    pardo_dofn(self.plants_test)

  def test_pardo_dofn_params(self):
    pardo_dofn_params(self.dofn_params_test)

  def test_pardo_side_inputs_singleton(self):
    pardo_side_inputs_singleton(self.plants_test)

  def test_pardo_side_inputs_iter(self):
    pardo_side_inputs_iter(self.valid_plants_test)

  def test_pardo_side_inputs_dict(self):
    pardo_side_inputs_dict(self.valid_plants_test)


if __name__ == '__main__':
  unittest.main()
