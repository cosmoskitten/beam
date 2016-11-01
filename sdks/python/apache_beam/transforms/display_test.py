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
from hamcrest.core.base_matcher import BaseMatcher

import apache_beam as beam
from apache_beam.transforms.display import HasDisplayData
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display import DisplayDataItem


class ItemMatcher(BaseMatcher):
  """ Matcher class for DisplayDataItems in unit tests.
  """
  def __init__(self, key, value, namespace, match_props=None):
    self.key = key
    self.value = value
    self.namespace = namespace

    if not match_props:
      raise ValueError('Must match at least one attribute.')
    self._match_props = match_props

  def _matches(self, item):
    if item.key != self.key and 'key' in self._match_props:
      return False
    if item.namespace != self.namespace and 'namespace' in self._match_props:
      return False
    if item.value != self.value and 'value' in self._match_props:
      return False
    return True

  def describe_to(self, description):
    if 'key' in self._match_props:
      description.append('key is {} '.format(self.key))
    if 'value' in self._match_props:
      description.append('value is {} '.format(self.value))
    if 'namespace' in self._match_props:
      description.append('namespace is {} '.format(self.namespace))

  @classmethod
  def matches_kv(cls, key, value):
    """ Create an item matcher that matches only key and value.
    """
    return cls(key=key,
               value=value,
               namespace=None,
               match_props=['key', 'value'])

  @classmethod
  def matches_kvn(cls, key, value, namespace):
    """ Create an item matcher that matches key, value and namespace.
    """
    return cls(key=key,
               value=value,
               namespace=namespace,
               match_props=['key', 'value', 'namespace'])


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
        ItemMatcher.matches_kvn('complex_url', 'github.com', nspace),
        ItemMatcher.matches_kvn('my_dd', now, nspace),
        ItemMatcher.matches_kvn('python_class', HasDisplayData, nspace),
        ItemMatcher.matches_kvn('static_integer', 120, nspace),
        ItemMatcher.matches_kvn('static_string', 'static me!', nspace)]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_drop_if_none(self):
    class MyDoFn(beam.DoFn):
      def display_data(self):
        return {'some_val': DisplayDataItem('something').drop_if_none(),
                'non_val': DisplayDataItem(None).drop_if_none(),
                'def_val': DisplayDataItem(True).drop_if_default(True),
                'nodef_val': DisplayDataItem(True).drop_if_default(False)}

    dd = DisplayData.create_from(MyDoFn())
    expected_items = [ItemMatcher.matches_kv('some_val', 'something'),
                      ItemMatcher.matches_kv('nodef_val', True)]
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
        ItemMatcher.matches_kvn('dofn_value', 42, dofn_nspace),
        ItemMatcher.matches_kvn('fn', SpecialDoFn, pardo_nspace)]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))


# TODO: Test __repr__ function
# TODO: Test PATH when added by swegner@
if __name__ == '__main__':
  unittest.main()
