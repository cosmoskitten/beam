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

"""This module contains Splittable DoFn logic that is specific to DirectRunner.
"""

from threading import Lock
from threading import Timer

import apache_beam as beam
from apache_beam import TimeDomain
from apache_beam import pvalue
from apache_beam.io.iobase import RestrictionTracker
from apache_beam.pipeline import PTransformOverride
from apache_beam.runners.common import DoFnContext
from apache_beam.runners.common import DoFnInvoker
from apache_beam.runners.common import DoFnSignature
from apache_beam.runners.common import OutputProcessor
from apache_beam.runners.direct.evaluation_context import DirectStepContext
from apache_beam.runners.direct.util import KeyedWorkItem
from apache_beam.runners.direct.watermark_manager import WatermarkManager
from apache_beam.runners.sdf_common import ElementAndRestriction
from apache_beam.runners.sdf_common import ProcessKeyedElements
from apache_beam.transforms.core import ProcessContinuation
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.trigger import _ValueStateTag
from apache_beam.utils.windowed_value import WindowedValue


class ProcessKeyedElementsViaKeyedWorkItemsOverride(PTransformOverride):
  """A transform override for ProcessElements transform."""

  def get_matcher(self):
    def _matcher(applied_ptransform):
      return isinstance(
          applied_ptransform.transform, ProcessKeyedElements)

    return _matcher

  def get_replacement_transform(self, ptransform):
    return ProcessKeyedElementsViaKeyedWorkItems(ptransform)


class ProcessKeyedElementsViaKeyedWorkItems(PTransform):
  """A transform that processes Splittable DoFn input via KeyedWorkItems.
  """

  def __init__(self, process_keyed_elements_transform):
    self._process_keyed_elements_transform = process_keyed_elements_transform

  def expand(self, pcoll):
    return pcoll | beam.core.GroupByKey() | ProcessElements(
        self._process_keyed_elements_transform)


class ProcessElements(PTransform):
  """Processes keyed input via Splittable DoFn objects."""

  def __init__(self, process_keyed_elements_transform):
    self._process_keyed_elements_transform = process_keyed_elements_transform
    self.sdf = self._process_keyed_elements_transform.sdf

  def expand(self, pcoll):
    return pvalue.PCollection(pcoll.pipeline)

  def new_process_fn(self, sdf):
    return ProcessFn(
        sdf,
        self._process_keyed_elements_transform.ptransform_args,
        self._process_keyed_elements_transform.ptransform_kwargs)


class ProcessFn(beam.DoFn):

  def __init__(
      self, sdf, args_for_invoker, kwargs_for_invoker):
    self.sdf = sdf
    self._element_tag = _ValueStateTag('element')
    self._restriction_tag = _ValueStateTag('restriction')
    self.watermark_hold_tag = _ValueStateTag('watermark_hold')
    self._process_element_invoker = None

    self.sdf_invoker = DoFnInvoker.create_invoker(
        DoFnSignature(self.sdf), context=DoFnContext('unused_context'),
        input_args=args_for_invoker, input_kwargs=kwargs_for_invoker)

  def set_step_context(self, step_context):
    assert isinstance(step_context, DirectStepContext)
    self._step_context = step_context

  def set_process_element_invoker(self, process_element_invoker):
    assert isinstance(process_element_invoker, SDFProcessElementInvoker)
    self._process_element_invoker = process_element_invoker

  def start_bundle(self):
    # TODO: support start_bundle() method for SDFs.
    pass

  def finish_bundle(self):
    # TODO: support finish_bundle() method for SDFs.
    pass

  def process(self, element, timestamp=beam.DoFn.TimestampParam,
              window=beam.DoFn.WindowParam, * args, **kwargs):
    if isinstance(element, KeyedWorkItem):
      # Must be a timer firing.
      key = element.encoded_key
    else:
      key, values = element
      values = list(values)
      assert len(values) == 1
      value = values[0]

    state = self._step_context.get_keyed_state(key)
    element_state = state.get_state(window, self._element_tag)
    is_seed_call = not element_state # Initially element_state is an empty list.

    if not is_seed_call:
      element = state.get_state(window, self._element_tag)
      restriction = state.get_state(window, self._restriction_tag)
      windowed_element = WindowedValue(element, timestamp, [window])
    else:
      element_and_restriction = (
          value.value if isinstance(value, WindowedValue) else value)
      assert isinstance(element_and_restriction, ElementAndRestriction)
      element = element_and_restriction.element
      restriction = element_and_restriction.restriction

      if isinstance(value, WindowedValue):
        windowed_element = WindowedValue(
            element, value.timestamp, value.windows)
      else:
        windowed_element = WindowedValue(element, timestamp, [window])

    tracker = self.sdf_invoker.invoke_create_tracker(restriction)
    assert self._process_element_invoker
    assert isinstance(self._process_element_invoker,
                      SDFProcessElementInvoker)

    output_values = self._process_element_invoker.invoke_process_element(
        self.sdf_invoker, windowed_element, tracker)

    sdf_result = None
    for output in output_values:
      if isinstance(output, SDFProcessElementInvoker.Result):
        # SDFProcessElementInvoker.Result should be the last item yielded.
        sdf_result = output
        break
      yield output

    if not sdf_result.residual_restriction:
      # All work for current residual and restriction pair is complete.
      state.clear_state(window, self._element_tag)
      state.clear_state(window, self._restriction_tag)
      # Releasing output watermark by setting it to positive infinity.
      state.add_state(window, self.watermark_hold_tag,
                      WatermarkManager.WATERMARK_POS_INF)
    else:
      state.add_state(window, self._element_tag, element)
      state.add_state(window, self._restriction_tag,
                      sdf_result.residual_restriction)
      state.add_state(window, self.watermark_hold_tag,
                      WatermarkManager.WATERMARK_NEG_INF)
      # Holding output watermark by setting it to negative infinity.
      state.set_timer(
          window, '', TimeDomain.WATERMARK, WatermarkManager.WATERMARK_NEG_INF)


class SDFProcessElementInvoker(object):
  """A utility that requsts checkpoints.

  Requests a checkpoint after processing a predefined number of elements or
  after a predefined time is elapsed from the process() invocation.
  """

  class Result(object):
    def __init__(
        self, residual_restriction=None, continuation=None,
        future_output_watermark=None):
      self.residual_restriction = residual_restriction
      self.continuation = continuation
      self.future_output_watermark = future_output_watermark

  def __init__(
      self, max_num_outputs, max_duration):
    self._max_num_outputs = max_num_outputs
    self._max_duration = max_duration
    self._checkpoint_lock = Lock()

  def test_method(self):
    raise ValueError

  def invoke_process_element(self, sdf_invoker, element, tracker):
    assert isinstance(sdf_invoker, DoFnInvoker)
    assert isinstance(tracker, RestrictionTracker)

    # Using lists for following two variables so that they are accessible from
    # the closure of the function initiate_checkpoint().
    checkpointed = []
    residual_restriction = []

    def initiate_checkpoint():
      with self._checkpoint_lock:
        if checkpointed:
          return
      residual_restriction.append(tracker.checkpoint())
      checkpointed.append(object())

    output_processor = _OutputProcessor()
    Timer(self._max_duration, initiate_checkpoint).start()
    sdf_invoker.invoke_process(
        element, restriction_tracker=tracker, output_processor=output_processor)

    assert output_processor.output_iter is not None
    output_count = 0

    # We have to expand and re-yield here to support ending execution for a
    # given number of output elements as well as to capture the
    # ProcessContinuation of one was returned.
    for output in output_processor.output_iter:
      if isinstance(output, ProcessContinuation):
        # Taking a checkpoint so that we can determine primary and residual
        # restrictions.
        initiate_checkpoint()

        # A ProcessContinuation should always be the last element produced by
        # the output iterator.
        # TODO: support continuing after the specified amount of delay.
        break

      yield output
      output_count += 1
      if self._max_num_outputs and output_count >= self._max_num_outputs:
        initiate_checkpoint()

    tracker.check_done()
    result = (
        SDFProcessElementInvoker.Result(
            residual_restriction=residual_restriction[0])
        if residual_restriction else SDFProcessElementInvoker.Result())
    yield result


class _OutputProcessor(OutputProcessor):

  def __init__(self):
    self.output_iter = None

  def process_outputs(self, windowed_input_element, output_iter):
    self.output_iter = output_iter
