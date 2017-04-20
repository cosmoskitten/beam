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

cimport cython

from apache_beam.utils.windowed_value cimport WindowedValue
from apache_beam.metrics.execution cimport ScopedMetricsContainer


cdef type OutputValue, TimestampedValue


cdef class Receiver(object):
  cpdef receive(self, WindowedValue windowed_value)


cdef class DoFnMethodWrapper(object):
  cdef public object args
  cdef public object defaults
  cdef object _method_value

  cpdef call(self, list args, dict kwargs)


cdef class DoFnSignature(object):
  cdef public DoFnMethodWrapper process_method
  cdef public DoFnMethodWrapper start_bundle_method
  cdef public DoFnMethodWrapper finish_bundle_method
  cdef public object do_fn


cdef class DoFnInvoker(object):

  cdef DoFnSignature signature

  cpdef invoke_process(self, WindowedValue element, process_output_fn)
  cpdef invoke_start_bundle(self, process_output_fn)
  cpdef invoke_finish_bundle(self, process_output_fn)

  # TODO(chamikara) define static method create_invoker() here.


cdef class SimpleInvoker(DoFnInvoker):
  pass


cdef class PerWindowInvoker(DoFnInvoker):

  cdef list side_inputs
  cdef DoFnContext context
  cdef list args
  cdef dict kwargs
  cdef list placeholders
  cdef bint has_windowed_inputs


cdef class DoFnRunner(Receiver):

  cdef object window_fn
  cdef DoFnContext context
  cdef object tagged_receivers
  cdef LoggingContext logging_context
  cdef object step_name
  cdef ScopedMetricsContainer scoped_metrics_container
  cdef list side_inputs
  cdef Receiver main_receivers
  cdef DoFnInvoker do_fn_invoker

  cpdef process(self, WindowedValue element)
  @cython.locals(windowed_value=WindowedValue)
  cpdef _process_outputs(self, WindowedValue element, results)


cdef class DoFnContext(object):
  cdef object label
  cdef object state
  cdef WindowedValue windowed_value
  cpdef set_element(self, WindowedValue windowed_value)


cdef class LoggingContext(object):
  # TODO(robertwb): Optimize "with [cdef class]"
  cpdef enter(self)
  cpdef exit(self)


cdef class _LoggingContextAdapter(LoggingContext):
  cdef object underlying


cdef class _ReceiverAdapter(Receiver):
  cdef object underlying
