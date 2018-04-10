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

""" For internal use only. No backwards compatibility guarantees."""

cimport cython
from libc.stdint cimport int64_t


# const int for number of buckets in every log10 bucket
cdef enum:
  buckets_per_10 = 3


cdef class DataflowDistributionCounter(object):
  cdef public int64_t min
  cdef public int64_t max
  cdef public int64_t count
  cdef public int64_t sum
  cdef int64_t first_bucket_offset
  cdef int64_t last_bucket_offset
  cdef int64_t* buckets
  cpdef bint add_input(self, int64_t element) except -1
  cdef int64_t calculate_bucket_index(self, int64_t element)
  cpdef object translate_to_histogram(self, histogram)
  cpdef bint add_inputs_for_test(self, elements) except -1
  cpdef int64_t calculate_bucket_index_for_test(self, int64_t element)
