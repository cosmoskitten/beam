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

cimport cython
from apache_beam.utils.counters import Counter
from apache_beam.utils.counters import CounterName

from cython.operator cimport dereference as dref
from cython.operator cimport preincrement as iref

cdef class Journal(object):
  def __init__(self):
    self.max_snapshot = -1
    self.journal = deque[SnapshottedExecutionPtr]()

  cdef void add_journal(self, ElementExecutionPtr execution_ptr,
                        int64_t snapshot):
    if snapshot <= self.max_snapshot:
      raise ValueError('Timestamps must be monotonically increasing.'
                       + 'Input snapshot %d is not greater than max snapshot %d'
                       % (snapshot, self.max_snapshot))
    cdef SnapshottedExecutionPtr snapshotted_ptr = \
      <SnapshottedExecutionPtr> PyMem_Malloc(sizeof(SnapshottedExecution))
    snapshotted_ptr.execution_ptr = execution_ptr
    snapshotted_ptr.snapshot = snapshot
    self.journal.push_back(snapshotted_ptr)
    self.max_snapshot = snapshot

cdef class ReaderWriterState(object):
  def __init__(self):
    self.latest_snapshot = 0
    self.execution_journal = Journal()
    self.done_journal = Journal()

cdef class ExecutionJournalReader(object):
  def __init__(self, ReaderWriterState shared_state):
    self.shared_state = shared_state
    self.execution_duration = unordered_map[ElementExecutionPtr, int64_t]()

  cdef void take_sample\
          (self, int64_t sample_time,
           unordered_map[CharPtr, vector[int64_t]]& counter_cache) nogil:
    latest_snapshot = self.shared_state.latest_snapshot
    self.attribute_processing_time(sample_time, latest_snapshot)
    self.update_counter_cache(latest_snapshot, counter_cache)

  cdef void attribute_processing_time(self, int64_t sample_time,
                                      int64_t latest_snapshot) nogil:
    cdef int64_t total_execution = 0
    cdef unordered_map[ElementExecutionPtr, int64_t] executions_per_element
    cdef SnapshottedExecutionPtr snapshotted_ptr = NULL
    while self.shared_state.execution_journal.journal.empty() is False:
      if self.shared_state.execution_journal.journal.front().snapshot \
          <= latest_snapshot:
        total_execution += 1
        snapshotted_ptr = self.shared_state.execution_journal.journal.front()
        self.shared_state.execution_journal.journal.pop_front()
        if snapshotted_ptr.execution_ptr == NULL:
          continue
        executions_per_element[snapshotted_ptr.execution_ptr] += 1
      else:
        break
    if snapshotted_ptr != NULL and snapshotted_ptr.snapshot == latest_snapshot:
      self.shared_state.execution_journal.journal.push_front(snapshotted_ptr)
    cdef unordered_map[ElementExecutionPtr, int64_t].iterator it = \
      executions_per_element.begin()
    cdef ElementExecutionPtr element_ptr = NULL
    cdef int64_t attribution_time = 0
    while it != executions_per_element.end():
      element_ptr = dref(it).first
      attribution_time = sample_time / total_execution \
                         * executions_per_element[element_ptr]
      self.execution_duration[element_ptr] += attribution_time
      iref(it)

  cdef void update_counter_cache\
          (self, int64_t latest_snapshot,
           unordered_map[CharPtr, vector[int64_t]]& counter_cache) nogil:
    cdef SnapshottedExecutionPtr snapshotted_ptr = NULL
    cdef ElementExecutionPtr element_ptr = NULL
    while self.shared_state.done_journal.journal.empty() is False:
      snapshotted_ptr = self.shared_state.done_journal.journal.front()
      if snapshotted_ptr.snapshot <= latest_snapshot:
        self.shared_state.done_journal.journal.pop_front()
        element_ptr = snapshotted_ptr.execution_ptr
        counter_cache[element_ptr.operation_name]\
          .push_back(self.execution_duration[element_ptr]/1000000)
      else:
        break


cdef class ExecutionJournalWriter(object):

  def __init__(self, ReaderWriterState shared_state):
    self.shared_state = shared_state
    self.execution_stack = deque[ElementExecutionPtr]()
    self.add_execution(NULL)

  cdef void add_execution(self, ElementExecutionPtr execution_ptr):
    cdef int64_t next_snapshot = self.shared_state.latest_snapshot + 1
    self.execution_stack.push_back(execution_ptr)
    self.shared_state.execution_journal\
      .add_journal(execution_ptr, next_snapshot)
    self.shared_state.latest_snapshot = next_snapshot

  cdef void start_processing(self, char* operation_name):
    cdef ElementExecutionPtr execution_ptr = \
      <ElementExecutionPtr> PyMem_Malloc(sizeof(ElementExecution))
    assert (execution_ptr != NULL)
    execution_ptr.operation_name = operation_name
    self.add_execution(execution_ptr)

  cdef void done_processing(self):
    if self.execution_stack.size() <= 1:
      raise ValueError('No processing elements currently tracked.')
    cdef ElementExecutionPtr last_execution = self.execution_stack.back()
    cdef ElementExecutionPtr next_execution = NULL
    self.execution_stack.pop_back()
    cdef int64_t next_snapshot = self.shared_state.latest_snapshot + 1
    self.shared_state.done_journal.add_journal(last_execution, next_snapshot)
    if self.execution_stack.empty() is False:
      next_execution = self.execution_stack.back()
    self.shared_state.execution_journal\
      .add_journal(next_execution, next_snapshot)
    self.shared_state.latest_snapshot = next_snapshot


cdef class DataflowElementExecutionTracker(object):

  def __init__(self):
    self.counter_cache = unordered_map[CharPtr, vector[int64_t]]()
    self.cache_start_index = unordered_map[CharPtr, int64_t]()
    self.shared_state = ReaderWriterState()
    self.execution_reader = ExecutionJournalReader(self.shared_state)
    self.execution_writer = ExecutionJournalWriter(self.shared_state)

  cdef void enter(self, char* operation_name):
    self.execution_writer.start_processing(operation_name)

  cdef void exit(self):
    self.execution_writer.done_processing()

  cdef void take_sample(self, int64_t nanos_sampling_duration) nogil:
    self.execution_reader\
      .take_sample(nanos_sampling_duration, self.counter_cache)

  cpdef void enter_for_test(self, char* operation_name):
    self.enter(operation_name)

  cpdef void exit_for_test(self):
    self.exit()

  cpdef void take_sample_for_test(self, int64_t nanos_sampling_duration):
      self.execution_reader\
        .take_sample(nanos_sampling_duration, self.counter_cache)

  cpdef report_counter(self, counter_factory):
    cdef unordered_map[CharPtr, vector[int64_t]].iterator map_it = \
      self.counter_cache.begin()
    cdef int64_t start_index = 0, end_index = 0
    cdef char* op_name
    cdef vector[int64_t] values
    while map_it != self.counter_cache.end():
      op_name = dref(map_it).first
      values = dref(map_it).second
      end_index = values.size()
      start_index = self.cache_start_index[op_name]
      counter_name = \
        CounterName('per-element-processing-time', step_name=op_name)
      time_counter = \
        counter_factory.get_counter(counter_name, Counter.DISTRIBUTION)
      while start_index < end_index:
        time_counter.update(values[start_index])
        start_index += 1
      self.cache_start_index[op_name] = end_index
      iref(map_it)
