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

"""User-facing interfaces for the Beam State and Timer APIs.

Experimental; no backwards-compatibility guarantees.
"""

from __future__ import absolute_import

import logging

from apache_beam.coders import Coder
from apache_beam.transforms.timeutil import TimeDomain


class StateSpec(object):
  """Specification for a user DoFn state cell."""

  def __init__(self):
    raise NotImplementedError

  def __repr__(self):
    return '%s(%s)' % (self.__class__.__name__, self.name)


class BagStateSpec(StateSpec):
  """Specification for a user DoFn bag state cell."""

  def __init__(self, name, coder):
    assert isinstance(name, str)
    assert isinstance(coder, Coder)
    self.name = name
    self.coder = coder


class CombiningValueStateSpec(StateSpec):
  """Specification for a user DoFn combining value state cell."""

  def __init__(self, name, coder, combiner):
    # Avoid circular import.
    from apache_beam.transforms.core import CombineFn

    assert isinstance(name, str)
    assert isinstance(coder, Coder)
    assert isinstance(combiner, CombineFn)
    self.name = name
    self.coder = coder
    self.combiner = combiner


class TimerSpec(object):
  """Specification for a user stateful DoFn timer."""

  def __init__(self, name, time_domain):
    self.name = name
    if time_domain not in (TimeDomain.WATERMARK, TimeDomain.REAL_TIME):
      raise ValueError('Unsupported TimeDomain: %r.' % (time_domain,))
    self.time_domain = time_domain

  def __repr__(self):
    return '%s(%s)' % (self.__class__.__name__, self.name)


# Attribute for attaching a TimerSpec to an on_timer method.
_TIMER_SPEC_ATTRIBUTE_NAME = '_on_timer_spec'


def on_timer(timer_spec):
  """Decorator for timer firing DoFn method.

  This decorator allows a user to specify an on_timer processing method
  in a stateful DoFn.  Sample usage:

  > class MyDoFn(DoFn):
  >   TIMER_SPEC = TimerSpec('timer', TimeDomain.WATERMARK)
  >
  >   @on_timer(TIMER_SPEC)
  >   def my_timer_expiry_callback(self):
  >     logging.info('Timer expired!')
  """

  if not isinstance(timer_spec, TimerSpec):
    raise ValueError('@on_timer decorator expected TimerSpec.')

  def _inner(method):
    if not callable(method):
      raise ValueError('@on_timer decorator expected callable.')
    setattr(method, _TIMER_SPEC_ATTRIBUTE_NAME, timer_spec)
    return method

  return _inner


class UserStateUtils(object):

  @staticmethod
  def get_on_timer_methods(dofn):
    # Avoid circular import.
    from apache_beam.transforms.core import DoFn
    if not isinstance(dofn, DoFn):
      raise ValueError('Expected DoFn.')

    timerid_to_methods = {}
    result = {}

    for name in dir(dofn):
      value = getattr(dofn, name, None)
      if (callable(value) and
          getattr(value, _TIMER_SPEC_ATTRIBUTE_NAME, None) is not None):
        timer_spec = getattr(value, _TIMER_SPEC_ATTRIBUTE_NAME)
        assert isinstance(timer_spec, TimerSpec)
        if timer_spec.name in timerid_to_methods:
          raise ValueError(
              'Duplicate on_timer method for timer of name %r.' %
              timer_spec.name)
        timerid_to_methods[timer_spec.name] = value
        result[timer_spec] = value

    return result

  @staticmethod
  def validate_stateful_dofn(dofn):
    # Avoid circular import.
    from apache_beam.runners.common import MethodWrapper
    from apache_beam.transforms.core import _DoFnParam
    from apache_beam.transforms.core import _StateDoFnParam
    from apache_beam.transforms.core import _TimerDoFnParam

    all_state_specs = set()
    all_timer_specs = set()

    method_names = ['process', 'start_bundle', 'finish_bundle']

    # Validate on_timer callbacks.
    timerspec_to_methods = UserStateUtils.get_on_timer_methods(dofn)
    all_timer_specs.update(timerspec_to_methods.keys())

    # Validate params to process(), start_bundle(), finish_bundle() and to
    # any on_timer callbacks.
    method_names += list(m.__name__ for m in timerspec_to_methods.values())
    for method_name in method_names:
      method = MethodWrapper(dofn, method_name)
      param_ids = list(d.param_id for d in method.defaults
                       if isinstance(d, _DoFnParam))
      if len(param_ids) != len(set(param_ids)):
        raise ValueError(
            'DoFn %r has duplicate %s method parameters: %s.' % (
                dofn, method_name, param_ids))
      for d in method.defaults:
        if isinstance(d, _StateDoFnParam):
          all_state_specs.add(d.state_spec)
        elif isinstance(d, _TimerDoFnParam):
          all_timer_specs.add(d.timer_spec)

    # Reject DoFns that have multiple state or timer specs with the same name.
    if len(all_state_specs) != len(set(s.name for s in all_state_specs)):
      raise ValueError(
          'DoFn %r has multiple StateSpecs with the same name: %s.' % (
              dofn, all_state_specs))
    if len(all_timer_specs) != len(set(s.name for s in all_timer_specs)):
      raise ValueError(
          'DoFn %r has multiple TimerSpecs with the same name: %s.' % (
              dofn, all_timer_specs))

    # Crawl for any additional timer specs. This step is only needed for
    # producing validation warnings. We produce a validation warning for each
    # timer that is specified without a corresponding on_timer callback. Such
    # cases are not semantically incorrect, but suggest user sloppiness and
    # possible error.
    for name in dir(dofn):
      value = getattr(dofn, name, None)
      if isinstance(value, TimerSpec):
        all_timer_specs.add(value)

    validation_warnings = []
    for timer_spec in all_timer_specs:
      if timer_spec not in timerspec_to_methods:
        warning = (
            ('In DoFn %s: timer spec %s does not have a corresponding '
             '@on_timer callback.') % (dofn, timer_spec))
        logging.warning(warning)
        validation_warnings.append(warning)

    # Return warnings for unit test usage.
    return validation_warnings
