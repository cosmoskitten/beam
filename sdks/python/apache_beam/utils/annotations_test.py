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

import unittest
import warnings

from apache_beam.utils.annotations import deprecated
from apache_beam.utils.annotations import experimental


class AnnotationTests(unittest.TestCase):
  # Note: use different names for each of the the functions decorated
  # so that a warning is produced for each of them.
  def test_deprecated_with_since_current_message(self):
    with warnings.catch_warnings(record=True) as w:
      @deprecated(since='v.1', current='multiply', extra_message='Do this')
      def fnc_test_deprecated_with_since_current_message():
        return 'lol'
      fnc_test_deprecated_with_since_current_message()
      self.check_annotation(
        warning=w, warning_size=1,
        warning_type=DeprecationWarning,
        obj_name='fnc_test_deprecated_with_since_current_message',
        annotation_type='deprecated',
        label_check_list=[('since', True),
                          ('instead', True),
                          ('Do this', True)])

  def test_deprecated_with_since_current(self):
    with warnings.catch_warnings(record=True) as w:
      @deprecated(since='v.1', current='multiply')
      def fnc_test_deprecated_with_since_current():
        return 'lol'
      fnc_test_deprecated_with_since_current()
      self.check_annotation(
        warning=w, warning_size=1,
        warning_type=DeprecationWarning,
        obj_name='fnc_test_deprecated_with_since_current',
        annotation_type='deprecated',
        label_check_list=[('since', True),
                          ('instead', True)])

  def test_deprecated_without_current(self):
    with warnings.catch_warnings(record=True) as w:
      @deprecated(since='v.1')
      def fnc_test_deprecated_without_current():
        return 'lol'
      fnc_test_deprecated_without_current()
      self.check_annotation(
        warning=w, warning_size=1,
        warning_type=DeprecationWarning,
        obj_name='fnc_test_deprecated_without_current',
        annotation_type='deprecated',
        label_check_list=[('since', True),
                          ('instead', False)])

  def test_deprecated_without_since_should_fail(self):
    with warnings.catch_warnings(record=True) as w:
      with self.assertRaises(TypeError):

        @deprecated()
        def fnc_test_deprecated_without_since_should_fail():
          return 'lol'
        fnc_test_deprecated_without_since_should_fail()
      assert not w

  def test_deprecated_without_since_custom_should_fail(self):
    with warnings.catch_warnings(record=True) as w:
      with self.assertRaises(TypeError):
        @deprecated(custom_message='Test %since%')
        def fnc_test_deprecated_without_since_custom_should_fail():
          return 'lol'
        fnc_test_deprecated_without_since_custom_should_fail()
      assert not w

  def test_experimental_with_current_message(self):
    with warnings.catch_warnings(record=True) as w:
      @experimental(current='multiply', extra_message='Do this')
      def fnc_test_experimental_with_current_message():
        return 'lol'
      fnc_test_experimental_with_current_message()
      self.check_annotation(
          warning=w, warning_size=1,
          warning_type=FutureWarning,
          obj_name='fnc_test_experimental_with_current_message',
          annotation_type='experimental',
          label_check_list=[('instead', True),
                            ('Do this', True)])

  def test_experimental_with_current(self):
    with warnings.catch_warnings(record=True) as w:
      @experimental(current='multiply')
      def fnc_test_experimental_with_current():
        return 'lol'
      fnc_test_experimental_with_current()
      self.check_annotation(
        warning=w, warning_size=1,
        warning_type=FutureWarning,
        obj_name='fnc_test_experimental_with_current',
        annotation_type='experimental',
        label_check_list=[('instead', True)])

  def test_experimental_without_current(self):
    with warnings.catch_warnings(record=True) as w:
      @experimental()
      def fnc_test_experimental_without_current():
        return 'lol'
      fnc_test_experimental_without_current()
      self.check_annotation(
        warning=w, warning_size=1,
        warning_type=FutureWarning,
        obj_name='fnc_test_experimental_without_current',
        annotation_type='experimental',
        label_check_list=[('instead', False)])

  def test_frequency(self):
    """Tests that the filter 'once' is sufficient to print once per
    warning independently of location."""
    with warnings.catch_warnings(record=True) as w:
      @experimental()
      def fnc_test_annotate_frequency():
        return 'lol'

      @experimental()
      def fnc2_test_annotate_frequency():
        return 'lol'
      fnc_test_annotate_frequency()
      fnc_test_annotate_frequency()
      fnc2_test_annotate_frequency()
      self.check_annotation(
        warning=[w[0]], warning_size=1,
        warning_type=FutureWarning,
        obj_name='fnc_test_annotate_frequency',
      annotation_type='experimental',
        label_check_list=[])
      self.check_annotation(
        warning=[w[1]], warning_size=1,
        warning_type=FutureWarning,
        obj_name='fnc2_test_annotate_frequency',
        annotation_type='experimental',
        label_check_list=[])

  def test_frequency_class(self):
    """Tests that the filter 'once' is sufficient to print once per
    warning independently of location."""
    with warnings.catch_warnings(record=True) as w:
      @experimental()
      class Class_test_annotate_frequency():
        fooo = 'lol'
        def foo(self):
            return 'lol'

      @experimental()
      class Class2_test_annotate_frequency():
        fooo = 'lol'
        def foo(self):
            return 'lol'
      foo = Class_test_annotate_frequency()
      foo.foo()
      foo1 = Class_test_annotate_frequency()
      foo1.foo()
      foo2 = Class2_test_annotate_frequency()
      foo2.foo()
      self.check_annotation(
        warning=[w[0]], warning_size=1,
        warning_type=FutureWarning,
        obj_name='Class_test_annotate_frequency',
        annotation_type='experimental',
        label_check_list=[])
      self.check_annotation(
        warning=[w[1]], warning_size=1,
        warning_type=FutureWarning,
        obj_name='Class2_test_annotate_frequency',
        annotation_type='experimental',
        label_check_list=[])


  def test_decapreted_custom_no_replacements(self):
    """Tests if custom message prints an empty string
    for each replacement string when only the
    custom_message and since parameter are given."""
    with warnings.catch_warnings(record=True) as w:
      strSince='v1'
      strCustom = 'Replacement:%since%%current%%extra%'
      @deprecated(since=strSince,custom_message=strCustom)
      def fnc_test_experimental_custom_no_replacements():
        return 'lol'
      fnc_test_experimental_custom_no_replacements()
      self.check_custom_annotation(
        warning=w, warning_size=1,
        warning_type=DeprecationWarning,
        obj_name='fnc_test_experimental_custom_no_replacements',
        annotation_type='experimental',
        intended_message=strCustom
        .replace('%since%',strSince)
        .replace('%current%','')
        .replace('%extra%',''))


  def test_enforce_custom_since_deprecated_must_fail(self):
    """Tests since replacement string inclusion on the
    custom message for the decapreted string. If no
    since replacement string is given, the annotation must fail"""
    with warnings.catch_warnings(record=True) as w:
      with self.assertRaises(TypeError):
        strSince='v1'
        strCustom = 'Replacement:'
        @deprecated(since=strSince,custom_message=strCustom)
        def fnc_test_experimental_custom_no_replacements():
          return 'lol'
        fnc_test_experimental_custom_no_replacements()
      assert not w


  def test_deprecated_with_since_current_message_custom(self):
    with warnings.catch_warnings(record=True) as w:
      strSince = 'v.1'
      strCurrent = 'multiply'
      strExtra = 'Do this'
      strCustom="%name% Will be deprecated from %since%. \
                  Please use %current% insted. Will %extra%"
      @deprecated(since=strSince, current=strCurrent, extra_message=strExtra,
                  custom_message=strCustom)
      def fnc_test_deprecated_with_since_current_message_custom():
        return 'lol'
      fnc_test_deprecated_with_since_current_message_custom()
      self.check_custom_annotation(
        warning=w, warning_size=1,
        warning_type=DeprecationWarning,
        obj_name='fnc_test_deprecated_with_since_current_message_custom',
        annotation_type='deprecated',
        intended_message=strCustom
        .replace('%name%', fnc_test_deprecated_with_since_current_message_custom.__name__)
        .replace('%since%', strSince)
        .replace('%current%', strCurrent)
        .replace('%extra%', strExtra))

  def test_deprecated_with_since_current_message_class(self):
    with warnings.catch_warnings(record=True) as w:
      @deprecated(since='v.1', current='multiply', extra_message='Do this')
      class Class_test_deprecated_with_since_current_message:
        fooo = 'lol'
        def foo(self):
          return 'lol'
      foo = Class_test_deprecated_with_since_current_message()
      foo.foo()
      self.check_annotation(
          warning=w, warning_size=1,
          warning_type=DeprecationWarning,
          obj_name='Class_test_deprecated_with_since_current_message',
          annotation_type='deprecated',
          label_check_list=[('since', True),
                            ('instead', True),
                            ('Do this', True)])


  def test_experimental_with_current_message_custom(self):
    with warnings.catch_warnings(record=True) as w:
      strCurrent='multiply'
      strExtra='DoThis'
      strCustom='%name% Function on experimental phase, use %current% \
      for stability. Will %extra%.'
      @experimental(current=strCurrent, extra_message=strExtra,
                    custom_message=strCustom)
      def fnc_test_experimental_with_current_message_custom():
        return 'lol'
      fnc_test_experimental_with_current_message_custom()
      self.check_custom_annotation(
        warning=w, warning_size=1,
        warning_type=FutureWarning,
        obj_name='fnc_test_experimental_with_current_message_custom',
        annotation_type='experimental',
        intended_message=strCustom
        .replace('%name%',fnc_test_experimental_with_current_message_custom.__name__)
        .replace('%current%', strCurrent)
        .replace('%extra%', strExtra))


  def test_experimental_with_current_message_class(self):
    with warnings.catch_warnings(record=True) as w:
      @experimental(current='multiply', extra_message='Do this')
      class Class_test_experimental_with_current_message():
        fooo = 'lol'
        def foo(self):
            return 'lol'
      foo = Class_test_experimental_with_current_message()
      foo.foo()
      self.check_annotation(
          warning=w, warning_size=1,
          warning_type=FutureWarning,
          obj_name='Class_test_experimental_with_current_message',
          annotation_type='experimental',
          label_check_list=[('instead', True),
                            ('Do this', True)])

  # helper function
  def check_annotation(self, warning, warning_size, warning_type, obj_name,
                       annotation_type, label_check_list):
    self.assertEqual(1, warning_size)
    self.assertTrue(issubclass(warning[-1].category, warning_type))
    self.assertIn(obj_name + ' is ' + annotation_type, str(warning[-1].message))
    for label in label_check_list:
      if label[1] is True:
        self.assertIn(label[0], str(warning[-1].message))
      else:
        self.assertNotIn(label[0], str(warning[-1].message))

  # Helper function for custom messages
  def check_custom_annotation(self, warning, warning_size, warning_type, obj_name,
                              annotation_type, intended_message):
    self.assertEqual(1, warning_size)
    self.assertTrue(issubclass(warning[-1].category, warning_type))
    self.assertIn(intended_message , str(warning[-1].message))

if __name__ == '__main__':
  unittest.main()
