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

"""Core PTransform subclasses, such as FlatMap, GroupByKey, and Map."""

from __future__ import absolute_import

import sys
import logging
import math
import heapq

try:
  import mmh3
except ImportError:
  logging.info('Python version >=3.0 uses buildin hash function.')

from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.core import *

__all__ = [
    'ApproximateUniqueGlobally',
    'ApproximateUniquePerKey',
]

class ApproximateUniqueGlobally(PTransform):
  """
  Hashes input elements and uses those to extrapolate the size of the entire
  set of hash values by assuming the rest of the hash values are as densely
  distributed as the sample space.

  Args:
    **kwargs: Accepts a single named argument "size" or "error".
    size: an int not smaller than 16, which we would use to estimate
    number of unique values.
    error: max estimation error, which is a float between 0.01
    and 0.50. If error is given, size will be calculated from error with
    _sample_size_from_estimation_error function.
  """

  _NO_VALUE_ERR_MSG = 'Either size or error should be set. Received {}.'
  _MULTI_VALUE_ERR_MSG = 'Either size or error should be set. ' \
                         'Received {size = %s, error = %s}.'
  _INPUT_SIZE_ERR_MSG = 'ApproximateUnique needs a size >= 16 for an error ' \
                        '<= 0.50. In general, the estimation error is about ' \
                        '2 / sqrt(sample_size). Received {size = %s}.'
  _INPUT_ERROR_ERR_MSG = 'ApproximateUnique needs an estimation error ' \
                         'between 0.01 and 0.50. Received {error = %s}.'

  def __init__(self, **kwargs):
    input_size = kwargs.pop('size', None)
    input_err = kwargs.pop('error', None)

    if None not in (input_size, input_err):
      raise ValueError(self._MULTI_VALUE_ERR_MSG % (input_size, input_err))
    elif input_size is None and input_err is None:
      raise ValueError(self._NO_VALUE_ERR_MSG)
    elif input_size is not None:
      if not isinstance(input_size, int) or input_size < 16:
        raise ValueError(self._INPUT_SIZE_ERR_MSG % (input_size))
      else:
        self._sample_size = input_size
        self._max_est_err = None
    else:
      if input_err < 0.01 or input_err > 0.5:
        raise ValueError(self._INPUT_ERROR_ERR_MSG % (input_err))
      else:
        self._sample_size = self._get_sample_size_from_est_error(input_err)
        self._max_est_err = input_err

  def expand(self, pcoll):
    return pcoll \
           | 'CountGlobalUniqueValues' \
           >> (CombineGlobally(ApproximateUniqueCombineDoFn(self._sample_size)))

  @staticmethod
  def _get_sample_size_from_est_error(est_err):
    return int(math.ceil(4.0 / math.pow(est_err, 2.0)))


class ApproximateUniquePerKey(ApproximateUniqueGlobally):

  def expand(self, pcoll):
    return pcoll \
           | 'CountPerKeyUniqueValues' \
           >> (CombinePerKey(ApproximateUniqueCombineDoFn(self._sample_size)))


class _LargestUnique(object):
  """
  An object to keep samples and calculate sample hash space. It is an
  accumulator of a combine function.
  """
  _HASH_SPACE_SIZE = 2.0 * sys.maxsize

  def __init__(self, sample_size):
    self._sample_size = sample_size
    self._min_hash = sys.maxsize
    self._sample_heap = []
    self._sample_set = set()

  def add(self, element):
    """
    :param an element from pcoll.
    :return: boolean type whether the value is in the heap

    Adds a value to the heap, returning whether the value is (large enough to
    be) in the heap.
    """
    if len(self._sample_heap) >= self._sample_size and element < self._min_hash:
      return False

    if element not in self._sample_set:
      self._sample_set.add(element)
      heapq.heappush(self._sample_heap, element)

      if len(self._sample_heap) > self._sample_size:
        temp = heapq.heappop(self._sample_heap)
        self._sample_set.remove(temp)
        self._min_hash = self._sample_heap[0]
      elif element < self._min_hash:
        self._min_hash = element

    return True

  def get_estimate(self):
    """
    :return: estimation of unique values

    If heap size is smaller than sample size, just return heap size.
    Otherwise, takes into account the possibility of hash collisions,
    which become more likely than not for 2^32 distinct elements.
    Note that log(1+x) ~ x for small x, so for sampleSize << maxHash
    log(1 - sampleSize/sampleSpace) / log(1 - 1/sampleSpace) ~ sampleSize
    and hence estimate ~ sampleSize * HASH_SPACE_SIZE / sampleSpace
    as one would expect.
    """

    if len(self._sample_heap) < self._sample_size:
      return len(self._sample_heap)
    else:
      sample_space_size = sys.maxsize - 1.0 * self._min_hash
      est = math.log1p(-self._sample_size/ sample_space_size) \
            / math.log1p(-1 / sample_space_size) \
            * self._HASH_SPACE_SIZE \
            / sample_space_size

      return int(round(est))


class ApproximateUniqueCombineDoFn(CombineFn):
  """
  ApproximateUniqueCombineDoFn computes an estimate of the number of
  unique values that were combined.
  """

  def __init__(self, sample_size):
    self._sample_size = sample_size

  def create_accumulator(self, *args, **kwargs):
    return _LargestUnique(self._sample_size)

  @staticmethod
  def add_input(accumulator, element, *args, **kwargs):
    try:
      if 'mmh3' in sys.modules:
        accumulator.add(mmh3.hash64(str(element))[1])
      else:
        accumulator.add(hash(str(element)))
      return accumulator
    except Exception as e:
      raise RuntimeError("Runtime exception: %s", e)

  def merge_accumulators(self, accumulators, *args, **kwargs):
    merged_accumulator = self.create_accumulator()
    for accumulator in accumulators:
      for i in accumulator._sample_heap:
        merged_accumulator.add(i)

    return merged_accumulator

  @staticmethod
  def extract_output(accumulator):
    return accumulator.get_estimate()
