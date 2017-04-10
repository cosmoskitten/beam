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

# cython: profile=True

"""Worker operations executor."""

import sys

from apache_beam.internal import util
from apache_beam.metrics.execution import ScopedMetricsContainer
from apache_beam.pvalue import SideOutputValue
from apache_beam.transforms import core
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms.window import WindowFn
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.windowed_value import WindowedValue
from contextlib import contextmanager

import logging


class LoggingContext(object):

  def enter(self):
    pass

  def exit(self):
    pass


class Receiver(object):
  """An object that consumes a WindowedValue.

  This class can be efficiently used to pass values between the
  sdk and worker harnesses.
  """

  def receive(self, windowed_value):
    raise NotImplementedError


class DoFnSignature(object):

  class Method(object):

    def __init__(self, method_value, args, defaults):
      self.args = args
      self.defaults = defaults
      self._method_value = method_value

    def __call__(self, *args, **kwargs):
      return self._method_value(*args, **kwargs)

  def __init__(self):
    self.process_method = None
    self.start_bundle_method = None
    self.finish_bundle_method = None
    self.initial_restriction_method = None
    self.new_tracker_method = None
    self.restriction_coder_method = None
    self.do_fn = None

  @staticmethod
  def create_signature(do_fn):
    assert isinstance(do_fn, core.DoFn)
    signature = DoFnSignature()

    def _create_do_fn_method(do_fn, method_name):
      arguments, _, _, defaults = do_fn.get_function_arguments(method_name)
      defaults = defaults if defaults else []
      method_value = getattr(do_fn, method_name)
      return DoFnSignature.Method(method_value, arguments, defaults)

    signature.do_fn = do_fn
    signature.process_method = _create_do_fn_method(do_fn, 'process')
    signature.start_bundle_method = _create_do_fn_method(do_fn, 'start_bundle')
    signature.finish_bundle_method = _create_do_fn_method(do_fn, 'finish_bundle')

    return signature


class DoFnInvoker(object):

  def __init__(self, signature):
    self.signature = signature

  @staticmethod
  def create_invoker(
      signature, use_simple_invoker, context, side_inputs, input_args, input_kwargs):
    if use_simple_invoker:
      return SimpleInvoker(signature)
    else:
      return PerWindowInvoker(signature, context, side_inputs, input_args, input_kwargs)

  def invoke_process(self, element, process_output_fn):
    raise NotImplementedError

  def invoke_start_bundle(self, process_output_fn):
    defaults = self.signature.start_bundle_method.defaults
    defaults = defaults if defaults else []
    args = [self.context if d == core.DoFn.ContextParam else d
            for d in defaults]
    process_output_fn(None, self.signature.start_bundle_method(*args))

  def invoke_finish_bundle(self, process_output_fn):
    defaults = self.signature.start_bundle_method.defaults
    defaults = defaults if defaults else []
    args = [self.context if d == core.DoFn.ContextParam else d
            for d in defaults]
    process_output_fn(None, self.signature.finish_bundle_method(*args))


class SimpleInvoker(DoFnInvoker):

  def invoke_process(self, element, process_output_fn):
    process_output_fn(element, self.signature.process_method(element.value))


class PerWindowInvoker(DoFnInvoker):

  def __init__(self, signature, context, side_inputs, input_args, input_kwargs):
    super(PerWindowInvoker, self).__init__(signature)
    self.side_inputs = side_inputs
    self.context = context
    self.has_windowed_inputs = not all(
        si.is_globally_windowed() for si in side_inputs)
    default_arg_values = signature.process_method.defaults
    self.has_windowed_inputs = (self.has_windowed_inputs or
                                core.DoFn.WindowParam in default_arg_values)

    # Try to prepare all the arguments that can just be filled in
    # without any additional work. in the process function.
    # Also cache all the placeholders needed in the process function.

    # Fill in sideInputs if they are globally windowed

    global_window = GlobalWindow()

    self.args = input_args if input_args else []
    self.kwargs = input_kwargs if input_kwargs else {}

    if not self.has_windowed_inputs:
      self.args, self.kwargs = util.insert_values_in_args(
          input_args, input_kwargs, [si[global_window] for si in side_inputs])

    arguments = signature.process_method.args
    defaults = signature.process_method.defaults

    # Create placeholder for element parameter of DoFn.process() method.
    self_in_args = int(signature.do_fn.is_process_bounded())

    class ArgPlaceholder(object):
      def __init__(self, placeholder):
        self.placeholder = placeholder

    if core.DoFn.ElementParam not in default_arg_values:
      args_to_pick = len(arguments) - len(default_arg_values) - 1 - self_in_args
      final_args = (
        [ArgPlaceholder(core.DoFn.ElementParam)] + self.args[:args_to_pick])
    else:
      args_to_pick = len(arguments) - len(defaults) - self_in_args
      final_args = self.args[:args_to_pick]

    # Fill the OtherPlaceholders for context, window or timestamp
    input_args = iter(self.args[args_to_pick:])
    for a, d in zip(arguments[-len(defaults):], defaults):
      if d == core.DoFn.ElementParam:
        final_args.append(ArgPlaceholder(d))
      elif d == core.DoFn.ContextParam:
        final_args.append(ArgPlaceholder(d))
      elif d == core.DoFn.WindowParam:
        final_args.append(ArgPlaceholder(d))
      elif d == core.DoFn.TimestampParam:
        final_args.append(ArgPlaceholder(d))
      elif d == core.DoFn.SideInputParam:
        # If no more args are present then the value must be passed via kwarg
        try:
          final_args.append(input_args.next())
        except StopIteration:
          if a not in self.kwargs:
            raise ValueError("Value for sideinput %s not provided" % a)
      else:
        # If no more args are present then the value must be passed via kwarg
        try:
          final_args.append(input_args.next())
        except StopIteration:
          pass
    final_args.extend(list(input_args))
    self.args = final_args

    # Stash the list of placeholder positions for performance
    self.placeholders = [(i, x.placeholder) for (i, x) in enumerate(self.args)
                         if isinstance(x, ArgPlaceholder)]

  def invoke_process(self, element, process_output_fn):
    self.context.set_element(element)
    # Call for the process function for each window if has windowed side inputs
    # or if the process accesses the window parameter. We can just call it once
    # otherwise as none of the arguments are changing
    if self.has_windowed_inputs and len(element.windows) != 1:
      for w in element.windows:
        self._invoke_per_window(
            WindowedValue(element.value, element.timestamp, (w,)),
            process_output_fn)
    else:
      self._invoke_per_window(element, process_output_fn)

  def _invoke_per_window(self, element, process_output_fn):
    if self.has_windowed_inputs:
      window, = element.windows
      args, kwargs = util.insert_values_in_args(
          self.args, self.kwargs, [si[window] for si in self.side_inputs])
    else:
      args, kwargs = self.args, self.kwargs
    # TODO(sourabhbajaj): Investigate why we can't use `is` instead of ==
    for i, p in self.placeholders:
      if p == core.DoFn.ElementParam:
        args[i] = element.value
      elif p == core.DoFn.ContextParam:
        args[i] = self.context
      elif p == core.DoFn.WindowParam:
        args[i] = window
      elif p == core.DoFn.TimestampParam:
        args[i] = element.timestamp


    if not kwargs:
      process_output_fn(element, self.signature.process_method(*args))
    else:
      process_output_fn(element, self.signature.process_method(
          *args, **kwargs))


class DoFnRunner(Receiver):
  """A helper class for executing ParDo operations.
  """

  def __init__(self,
               fn,
               args,
               kwargs,
               side_inputs,
               windowing,
               context=None,
               tagged_receivers=None,
               logger=None,
               step_name=None,
               # Preferred alternative to logger
               # TODO(robertwb): Remove once all runners are updated.
               logging_context=None,
               # Preferred alternative to context
               # TODO(robertwb): Remove once all runners are updated.
               state=None,
               scoped_metrics_container=None):
    """Initializes a DoFnRunner.

    Args:
      fn: user DoFn to invoke
      args: positional side input arguments (static and placeholder), if any
      kwargs: keyword side input arguments (static and placeholder), if any
      side_inputs: list of sideinput.SideInputMaps for deferred side inputs
      windowing: windowing properties of the output PCollection(s)
      context: a DoFnContext to use (deprecated)
      tagged_receivers: a dict of tag name to Receiver objects
      logger: a logging module (deprecated)
      step_name: the name of this step
      logging_context: a LoggingContext object
      state: handle for accessing DoFn state
      scoped_metrics_container: Context switcher for metrics container
    """
    self.tagged_receivers = tagged_receivers
    self.scoped_metrics_container = (scoped_metrics_container
                                     or ScopedMetricsContainer())

    # Optimize for the common case.
    self.main_receivers = as_receiver(tagged_receivers[None])

    self.step_name = step_name

    # Need to support multiple iterations.
    side_inputs = list(side_inputs)

    if logging_context:
      self.logging_context = logging_context
    else:
      self.logging_context = get_logging_context(logger, step_name=step_name)

    # TODO(sourabh): Deprecate the use of context
    if state:
      assert context is None
      context = DoFnContext(step_name, state=state)
    else:
      assert context is not None
      context = context

    self.context = context

    do_fn_signature = DoFnSignature.create_signature(fn)

    default_arg_values = do_fn_signature.process_method.defaults
    use_simple_invoker = (
        not side_inputs and not args and not kwargs and not default_arg_values)

    self.window_fn = windowing.windowfn

    self.do_fn_invoker = DoFnInvoker.create_invoker(
        do_fn_signature, use_simple_invoker, context, side_inputs, args, kwargs)

  def receive(self, windowed_value):
    self.process(windowed_value)

  def process(self, windowed_value):
    self._invoke_process_method(windowed_value)

  @contextmanager
  def _invoke_with_context(self):
    try:
      self.logging_context.enter()
      self.scoped_metrics_container.enter()
      self.context.set_element(None)
      yield None
    except BaseException as exn:
      self.reraise_augmented(exn)
    finally:
      self.scoped_metrics_container.exit()
      self.logging_context.exit()

  def start(self):
    with self._invoke_with_context():
      self.do_fn_invoker.invoke_start_bundle(self.process_outputs)

  def finish(self):
    with self._invoke_with_context():
      self.do_fn_invoker.invoke_finish_bundle(self.process_outputs)

  def _invoke_process_method(self, element):
    with self._invoke_with_context():
      self.do_fn_invoker.invoke_process(element, self.process_outputs)

  def reraise_augmented(self, exn):
    if getattr(exn, '_tagged_with_step', False) or not self.step_name:
      raise
    args = exn.args
    if args and isinstance(args[0], str):
      args = (args[0] + " [while running '%s']" % self.step_name,) + args[1:]
      # Poor man's exception chaining.
      raise type(exn), args, sys.exc_info()[2]
    else:
      raise

  def process_outputs(self, windowed_input_element, results):
    """Dispatch the result of computation to the appropriate receivers.

    A value wrapped in a SideOutputValue object will be unwrapped and
    then dispatched to the appropriate indexed output.
    """
    if results is None:
      return
    for result in results:
      tag = None
      if isinstance(result, SideOutputValue):
        tag = result.tag
        if not isinstance(tag, basestring):
          raise TypeError('In %s, tag %s is not a string' % (self, tag))
        result = result.value
      if isinstance(result, WindowedValue):
        windowed_value = result
        if (windowed_input_element is not None
            and len(windowed_input_element.windows) != 1):
          windowed_value.windows *= len(windowed_input_element.windows)
      elif windowed_input_element is None:
        # Start and finish have no element from which to grab context,
        # but may emit elements.
        if isinstance(result, TimestampedValue):
          value = result.value
          timestamp = result.timestamp
          assign_context = NoContext(value, timestamp)
        else:
          value = result
          timestamp = -1
          assign_context = NoContext(value)
        windowed_value = WindowedValue(
            value, timestamp, self.window_fn.assign(assign_context))
      elif isinstance(result, TimestampedValue):
        assign_context = WindowFn.AssignContext(result.timestamp,
                                                result.value)
        windowed_value = WindowedValue(
            result.value, result.timestamp,
            self.window_fn.assign(assign_context))
        if len(windowed_input_element.windows) != 1:
          windowed_value.windows *= len(windowed_input_element.windows)
      else:
        windowed_value = windowed_input_element.with_value(result)
      if tag is None:
        self.main_receivers.receive(windowed_value)
      else:
        self.tagged_receivers[tag].output(windowed_value)


class NoContext(WindowFn.AssignContext):
  """An uninspectable WindowFn.AssignContext."""
  NO_VALUE = object()

  def __init__(self, value, timestamp=NO_VALUE):
    self.value = value
    self._timestamp = timestamp

  @property
  def timestamp(self):
    if self._timestamp is self.NO_VALUE:
      raise ValueError('No timestamp in this context.')
    else:
      return self._timestamp

  @property
  def existing_windows(self):
    raise ValueError('No existing_windows in this context.')


class DoFnState(object):
  """Keeps track of state that DoFns want, currently, user counters.
  """

  def __init__(self, counter_factory):
    self.step_name = ''
    self._counter_factory = counter_factory

  def counter_for(self, aggregator):
    """Looks up the counter for this aggregator, creating one if necessary."""
    return self._counter_factory.get_aggregator_counter(
        self.step_name, aggregator)


# TODO(robertwb): Replace core.DoFnContext with this.
class DoFnContext(object):

  def __init__(self, label, element=None, state=None):
    self.label = label
    self.state = state
    if element is not None:
      self.set_element(element)

  def set_element(self, windowed_value):
    self.windowed_value = windowed_value

  @property
  def element(self):
    if self.windowed_value is None:
      raise AttributeError('element not accessible in this context')
    else:
      return self.windowed_value.value

  @property
  def timestamp(self):
    if self.windowed_value is None:
      raise AttributeError('timestamp not accessible in this context')
    else:
      return self.windowed_value.timestamp

  @property
  def windows(self):
    if self.windowed_value is None:
      raise AttributeError('windows not accessible in this context')
    else:
      return self.windowed_value.windows


# TODO(robertwb): Remove all these adapters once service is updated out.
class _LoggingContextAdapter(LoggingContext):

  def __init__(self, underlying):
    self.underlying = underlying

  def enter(self):
    self.underlying.enter()

  def exit(self):
    self.underlying.exit()


def get_logging_context(maybe_logger, **kwargs):
  if maybe_logger:
    maybe_context = maybe_logger.PerThreadLoggingContext(**kwargs)
    if isinstance(maybe_context, LoggingContext):
      return maybe_context
    else:
      return _LoggingContextAdapter(maybe_context)
  else:
    return LoggingContext()


class _ReceiverAdapter(Receiver):

  def __init__(self, underlying):
    self.underlying = underlying

  def receive(self, windowed_value):
    self.underlying.output(windowed_value)


def as_receiver(maybe_receiver):
  if isinstance(maybe_receiver, Receiver):
    return maybe_receiver
  else:
    return _ReceiverAdapter(maybe_receiver)
