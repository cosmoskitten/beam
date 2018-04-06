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
from libc.stdint cimport int64_t, INT64_MAX
from libc.stdlib cimport malloc, free

cdef extern from "stdlib.h" nogil:
  void *memset(void *str, int c, size_t n)

cdef int64_t* MAX_LONG_10_FOR_LEADING_ZEROS = [19, 18, 18, 18, 18, 17, 17, 17,
                                               16, 16, 16, 15,15, 15, 15, 14,
                                               14, 14, 13, 13, 13, 12, 12, 12,
                                               12, 11, 11, 11, 10, 10, 10, 9, 9,
                                               9, 9, 8, 8,8, 7, 7, 7, 6, 6, 6,
                                               6, 5, 5, 5, 4, 4, 4, 3, 3, 3, 3,
                                               2, 2, 2, 1, 1, 1, 0, 0, 0]

cdef unsigned long long* POWER_TEN = [10e-1, 10e0, 10e1, 10e2, 10e3, 10e4, 10e5,
                                      10e6, 10e7, 10e8, 10e9, 10e10, 10e11,
                                      10e12, 10e13, 10e14, 10e15, 10e16, 10e17,
                                      10e18]


cdef inline bint compare_to(int64_t x, int64_t y):
  """return the sign bit of x-y"""
  if x < y:
    return 1
  return 0


cdef int64_t bit_length(int64_t element):
  """Same function as python built_in int.bit_length"""
  cdef int64_t bit_count = 0
  while element > 0:
    bit_count += 1
    element >>= 1
  return bit_count


cdef int64_t get_log10_round_to_floor(int64_t element):
  cdef int64_t number_of_leading_zeros = 64 - bit_length(element)
  cdef int64_t y = MAX_LONG_10_FOR_LEADING_ZEROS[number_of_leading_zeros]
  return y - compare_to(element, POWER_TEN[y])


cdef class DistributionAccumulator(object):
  """Distribution Counter:
  contains value distribution statistics and methods for incrementing
  Attributes:
    min: minimum value of all inputs.
    max: maximum value of all inputs.
    count: total count of all inputs.
    sum: sum of all inputs.
    first_bucket_offset: starting index of the first stored bucket.
    last_bucket_offset: end index of buckets.
    buckets: histogram buckets of value counts for a distribution
                                                    (1,2,5 bucketing).
             max bucket_index is 58( sys.maxint as input)
    buckets_per_10: 3 buckets for every power of ten -> 1, 2, 5
  """
  def __init__(self):
    self.min = INT64_MAX
    self.max = 0
    self.count = 0
    self.sum = 0
    self.first_bucket_offset = 58
    self.last_bucket_offset = 0
    self.buckets = <int64_t*> malloc(sizeof(int64_t) * 59)
    memset(self.buckets, 0, sizeof(int64_t) * 59)
    self.buckets_per_10 = 3

  def __dealloc__(self):
    """free allocated memory"""
    free(self.buckets)

  cdef bint add_input(self, int64_t element) except -1:
    if element < 0:
      raise ValueError('Distribution counters support only non-negative value')
    self.min = min(self.min, element)
    self.max = max(self.max, element)
    self.count += 1
    self.sum += element
    cdef int64_t bucket_index = self.calculate_bucket_index(element)
    self.buckets[bucket_index] += 1
    self.first_bucket_offset = min(self.first_bucket_offset, bucket_index)
    self.last_bucket_offset = max(self.last_bucket_offset, bucket_index)

  cdef int64_t calculate_bucket_index(self, int64_t element):
    """Calculate the bucket index for the given element"""
    if element == 0:
      return 0
    cdef int64_t log10_floor = get_log10_round_to_floor(element)
    cdef int64_t power_of_ten = POWER_TEN[log10_floor]
    cdef int64_t bucket_offset = 0
    if element <  power_of_ten<<1:
      bucket_offset = 0
    elif element < ((power_of_ten<<2) + power_of_ten):
      bucket_offset = 1
    else:
      bucket_offset = 2
    return 1 + log10_floor * self.buckets_per_10 + bucket_offset

  cpdef object get_add_input_fn(self):
    """Get add_input function"""
    return self.add_input

  cpdef object translate_to_histogram(self, histogram):
    """Translate buckets into Histogram"""
    histogram.firstBucketOffset = self.first_bucket_offset
    histogram.bucketCounts = []
    cdef int64_t index = self.first_bucket_offset
    for index in range(self.first_bucket_offset, self.last_bucket_offset+1):
      histogram.bucketCounts.append(self.buckets[index])

  cpdef bint add_inputs_for_test(self, elements) except -1:
    """ONLY used for unit tests"""
    for element in elements:
      self.add_input(element)

  cpdef int64_t calculate_bucket_index_for_test(self, int64_t element):
    """ONLY used for unit tests"""
    return self.calculate_bucket_index(element)
