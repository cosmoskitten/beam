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

"""Unit tests for the DisplayData API."""

from __future__ import absolute_import

from datetime import datetime
import unittest

import hamcrest as hc

import apache_beam as beam
from apache_beam.transforms.display import *


def make_nspace_display_data(component):
  dd = DisplayData.create_from(component)
  nspace = '{}.{}'.format(component.__module__, component.__class__.__name__)
  return nspace, dd


class DisplayDataTest(unittest.TestCase):

  def test_inheritance_ptransform(self):
    class MyTransform(beam.PTransform):
      pass

    display_pt = MyTransform()
    # PTransform inherits from HasDisplayData.
    self.assertTrue(isinstance(display_pt, HasDisplayData))
    self.assertEqual(display_pt.display_data(), {})

  def test_inheritance_dofn(self):
    class MyDoFn(beam.DoFn):
      pass

    display_dofn = MyDoFn()
    self.assertTrue(isinstance(display_dofn, HasDisplayData))
    self.assertEqual(display_dofn.display_data(), {})

  def test_base_cases(self):
    """ Tests basic display data cases (key:value, key:dict)
    It does not test subcomponent inclusion
    """
    class MyDoFn(beam.DoFn):
      def __init__(self, my_display_data=None):
        self.my_display_data = my_display_data

      def process(self, context):
        yield context.element + 1

      def display_data(self):
        return {'static_integer': 120,
                'static_string': 'static me!',
                'complex_url': DisplayDataItem('github.com',
                                               url='http://github.com',
                                               label='The URL'),
                'python_class': HasDisplayData,
                'my_dd': self.my_display_data}

    now = datetime.now()
    fn = MyDoFn(my_display_data=now)
    dd = DisplayData.create_from(fn)

    nspace = '{}.{}'.format(fn.__module__, fn.__class__.__name__)
    expected_items = [
        DisplayDataItem('github.com', label='The URL', url='http://github.com',
                        namespace=nspace, key='complex_url'),
        DisplayDataItem(now, namespace=nspace, key='my_dd'),
        DisplayDataItem(HasDisplayData, key='python_class', namespace=nspace),
        DisplayDataItem(120, namespace=nspace, key='static_integer'),
        DisplayDataItem('static me!', namespace=nspace, key='static_string')]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_subcomponent(self):
    class SpecialDoFn(beam.DoFn):
      def display_data(self):
        return {'dofn_value': 42}

    dofn = SpecialDoFn()
    pardo = beam.ParDo(dofn)
    dd = DisplayData.create_from(pardo)
    dofn_nspace = '{}.{}'.format(dofn.__module__, dofn.__class__.__name__)
    pardo_nspace = '{}.{}'.format(pardo.__module__, pardo.__class__.__name__)
    expected_items = [
        DisplayDataItem(42, key='dofn_value', namespace=dofn_nspace),
        DisplayDataItem(SpecialDoFn, key='fn', namespace=pardo_nspace,
                        label='Transform Function')]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))


# TODO: Test __repr__ function
# TODO: Test PATH when added by swegner@
if __name__ == '__main__':
  unittest.main()
