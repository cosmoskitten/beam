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

import types
import unittest
from functools import wraps

from apache_beam.utils.function_alias import function_alias


class FunctionAliasTest(unittest.TestCase):

  def test_function_alias(self):

    def f(a, b=2):
      """Some docstring"""
      return a + b

    g = function_alias(f, 'g')

    self.assertIsInstance(g, types.FunctionType)
    self.assertEqual(g.__name__, 'g')
    self.assertEqual(f.__name__, 'f')
    self.assertNotEqual(id(f), id(g))
    self.assertEqual(f.__code__, g.__code__)
    self.assertEqual(f.__globals__, g.__globals__)
    self.assertEqual(f.__defaults__, g.__defaults__)
    self.assertEqual(f.__closure__, g.__closure__)
    self.assertEqual(f.__doc__, g.__doc__)

  def test_function_alias_with_decorator(self):

    def dummy_decorator(fn):
      @wraps(fn)
      def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)
      return wrapper

    @dummy_decorator
    def f(a, b=2):
      """Some docstring"""
      return a + b

    g = function_alias(f, 'g')

    self.assertIsInstance(g, types.FunctionType)
    self.assertEqual(g.__name__, 'g')
    self.assertEqual(f.__name__, 'f')
    self.assertNotEqual(id(f), id(g))
    self.assertEqual(f.__code__, g.__code__)
    self.assertEqual(f.__globals__, g.__globals__)
    self.assertEqual(f.__defaults__, g.__defaults__)
    self.assertEqual(f.__closure__, g.__closure__)
    self.assertEqual(f.__doc__, g.__doc__)


if __name__ == '__main__':
  unittest.main()
