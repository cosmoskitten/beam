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

"""A library of basic cythonized CombineFn subclasses.

For internal use only; no backwards-compatibility guarantees.
"""

from __future__ import absolute_import

from apache_beam.transforms import core


class AccumulatorCombineFn(core.CombineFn):
  # singleton?
  def create_accumulator(self):
    return self._accumulator_type()

  @staticmethod
  def add_input(accumulator, element):
    accumulator.add_input(element)
    return accumulator

  def merge_accumulators(self, accumulators):
    accumulator = self._accumulator_type()
    accumulator.merge(accumulators)
    return accumulator

  @staticmethod
  def extract_output(accumulator):
    return accumulator.extract_output()

  def __eq__(self, other):
    return (isinstance(other, AccumulatorCombineFn)
            and self._accumulator_type is other._accumulator_type)

  def __hash__(self):
    return hash(self._accumulator_type)


_63 = 63  # Avoid large literals in C source code.
globals()['INT64_MAX'] = 2**_63 - 1
globals()['INT64_MIN'] = -2**_63


class CountAccumulator(object):
  def __init__(self):
    self.value = 0

  def add_input(self, unused_element):
    self.value += 1

  def merge(self, accumulators):
    for accumulator in accumulators:
      self.value += accumulator.value

  def extract_output(self):
    return self.value


class SumInt64Accumulator(object):
  def __init__(self):
    self.value = 0

  def add_input(self, element):
    global INT64_MAX, INT64_MIN  # pylint: disable=global-variable-not-assigned
    element = int(element)
    if not INT64_MIN <= element <= INT64_MAX:
      raise OverflowError(element)
    self.value += element

  def merge(self, accumulators):
    for accumulator in accumulators:
      self.value += accumulator.value

  def extract_output(self):
    if not INT64_MIN <= self.value <= INT64_MAX:
      self.value %= 2**64
      if self.value >= INT64_MAX:
        self.value -= 2**64
    return self.value


class MinInt64Accumulator(object):
  def __init__(self):
    self.value = INT64_MAX

  def add_input(self, element):
    element = int(element)
    if not INT64_MIN <= element <= INT64_MAX:
      raise OverflowError(element)
    if element < self.value:
      self.value = element

  def merge(self, accumulators):
    for accumulator in accumulators:
      if accumulator.value < self.value:
        self.value = accumulator.value

  def extract_output(self):
    return self.value


class MaxInt64Accumulator(object):
  def __init__(self):
    self.value = INT64_MIN

  def add_input(self, element):
    element = int(element)
    if not INT64_MIN <= element <= INT64_MAX:
      raise OverflowError(element)
    if element > self.value:
      self.value = element

  def merge(self, accumulators):
    for accumulator in accumulators:
      if accumulator.value > self.value:
        self.value = accumulator.value

  def extract_output(self):
    return self.value


class MeanInt64Accumulator(object):
  def __init__(self):
    self.sum = 0
    self.count = 0

  def add_input(self, element):
    element = int(element)
    if not INT64_MIN <= element <= INT64_MAX:
      raise OverflowError(element)
    self.sum += element
    self.count += 1

  def merge(self, accumulators):
    for accumulator in accumulators:
      self.sum += accumulator.sum
      self.count += accumulator.count

  def extract_output(self):
    if not INT64_MIN <= self.sum <= INT64_MAX:
      self.sum %= 2**64
      if self.sum >= INT64_MAX:
        self.sum -= 2**64
    return self.sum / self.count if self.count else _NAN


class CountCombineFn(AccumulatorCombineFn):
  _accumulator_type = CountAccumulator


class SumInt64Fn(AccumulatorCombineFn):
  _accumulator_type = SumInt64Accumulator


class MinInt64Fn(AccumulatorCombineFn):
  _accumulator_type = MinInt64Accumulator


class MaxInt64Fn(AccumulatorCombineFn):
  _accumulator_type = MaxInt64Accumulator


class MeanInt64Fn(AccumulatorCombineFn):
  _accumulator_type = MeanInt64Accumulator


_POS_INF = float('inf')
_NEG_INF = float('-inf')
_NAN = float('nan')


class SumDoubleAccumulator(object):
  def __init__(self):
    self.value = 0

  def add_input(self, element):
    element = float(element)
    self.value += element

  def merge(self, accumulators):
    for accumulator in accumulators:
      self.value += accumulator.value

  def extract_output(self):
    return self.value


class MinDoubleAccumulator(object):
  def __init__(self):
    self.value = _POS_INF

  def add_input(self, element):
    element = float(element)
    if element < self.value:
      self.value = element

  def merge(self, accumulators):
    for accumulator in accumulators:
      if accumulator.value < self.value:
        self.value = accumulator.value

  def extract_output(self):
    return self.value


class MaxDoubleAccumulator(object):
  def __init__(self):
    self.value = _NEG_INF

  def add_input(self, element):
    element = float(element)
    if element > self.value:
      self.value = element

  def merge(self, accumulators):
    for accumulator in accumulators:
      if accumulator.value > self.value:
        self.value = accumulator.value

  def extract_output(self):
    return self.value


class MeanDoubleAccumulator(object):
  def __init__(self):
    self.sum = 0
    self.count = 0

  def add_input(self, element):
    element = float(element)
    self.sum += element
    self.count += 1

  def merge(self, accumulators):
    for accumulator in accumulators:
      self.sum += accumulator.sum
      self.count += accumulator.count

  def extract_output(self):
    return self.sum / self.count if self.count else _NAN


class SumFloatFn(AccumulatorCombineFn):
  _accumulator_type = SumDoubleAccumulator


class MinFloatFn(AccumulatorCombineFn):
  _accumulator_type = MinDoubleAccumulator


class MaxFloatFn(AccumulatorCombineFn):
  _accumulator_type = MaxDoubleAccumulator


class MeanFloatFn(AccumulatorCombineFn):
  _accumulator_type = MeanDoubleAccumulator


class AllAccumulator(object):
  def __init__(self):
    self.value = True

  def add_input(self, element):
    self.value &= not not element

  def merge(self, accumulators):
    for accumulator in accumulators:
      self.value &= accumulator.value

  def extract_output(self):
    return self.value


class AnyAccumulator(object):
  def __init__(self):
    self.value = False

  def add_input(self, element):
    self.value |= not not element

  def merge(self, accumulators):
    for accumulator in accumulators:
      self.value |= accumulator.value

  def extract_output(self):
    return self.value


class AnyCombineFn(AccumulatorCombineFn):
  _accumulator_type = AnyAccumulator


class AllCombineFn(AccumulatorCombineFn):
  _accumulator_type = AllAccumulator


POWER_TEN = [10e-1, 10e0, 10e1, 10e2, 10e3, 10e4, 10e5,
             10e6, 10e7, 10e8, 10e9, 10e10, 10e11,
             10e12, 10e13, 10e14, 10e15, 10e16, 10e17,
             10e18]


def get_log10_round_to_floor(element):
  power = 0
  while element >= POWER_TEN[power]:
    power += 1
  return power - 1


class DataflowDistributionCounter(object):
  """Pure python DataflowDistributionCounter in case Cython not available.


  Please avoid using python mode if possible, since it's super slow
  Cythonized DatadflowDistributionCounter defined in
  apache_beam.transforms.cy_dataflow_distribution_counter.

  Attributes:
    min: minimum value of all inputs.
    max: maximum value of all inputs.
    count: total count of all inputs.
    sum: sum of all inputs.
    first_bucket_offset: starting index of the first stored bucket.
    last_bucket_offset: end index of buckets.
    buckets: histogram buckets of value counts for a
    distribution(1,2,5 bucketing). Max bucket_index is 58( sys.maxint as input).
    is_cythonized: mark whether DataflowDistributionCounter cythonized.
  """
  # Assume the max input is sys.maxint, then the possible max bucket size is 59
  MAX_BUCKET_SIZE = 59

  # 3 buckets for every power of ten -> 1, 2, 5
  BUCKET_PER_TEN = 3

  def __init__(self):
    self.min = INT64_MAX
    self.max = 0
    self.count = 0
    self.sum = 0
    self.first_bucket_offset = self.MAX_BUCKET_SIZE - 1
    self.last_bucket_offset = 0
    self.buckets = [0] * self.MAX_BUCKET_SIZE
    self.is_cythonized = False

  def add_input(self, element):
    if element < 0:
      raise ValueError('Distribution counters support only non-negative value')
    self.min = min(self.min, element)
    self.max = max(self.max, element)
    self.count += 1
    self.sum += element
    bucket_index = self.calculate_bucket_index(element)
    self.buckets[bucket_index] += 1
    self.first_bucket_offset = min(self.first_bucket_offset, bucket_index)
    self.last_bucket_offset = max(self.last_bucket_offset, bucket_index)

  def calculate_bucket_index(self, element):
    """Calculate the bucket index for the given element."""
    if element == 0:
      return 0
    log10_floor = get_log10_round_to_floor(element)
    power_of_ten = POWER_TEN[log10_floor]
    if element < power_of_ten * 2:
      bucket_offset = 0
    elif element < power_of_ten * 5:
      bucket_offset = 1
    else:
      bucket_offset = 2
    return 1 + log10_floor * self.BUCKET_PER_TEN + bucket_offset

  def translate_to_histogram(self, histogram):
    """Translate buckets into Histogram.

    Args:
      histogram: apache_beam.runners.dataflow.internal.clents.dataflow.Histogram
      Ideally, only call this function when reporting counter to
      dataflow service.
    """
    histogram.firstBucketOffset = self.first_bucket_offset
    histogram.bucketCounts = (
        self.buckets[self.first_bucket_offset:self.last_bucket_offset + 1])


class DataflowDistributionCounterFn(AccumulatorCombineFn):
  """A subclass of cy_combiners.AccumulatorCombineFn.

  Make DataflowDistributionCounter able to report to Dataflow service via
  CounterFactory.

  When cythonized DataflowDistributinoCounter available, make
  CounterFn combine with cythonized module, otherwise, combine with python
  version.
  """
  try:
    from apache_beam.transforms.cy_dataflow_distribution_counter import DataflowDistributionCounter
    _accumulator_type = DataflowDistributionCounter
  except ImportError:
    _accumulator_type = DataflowDistributionCounter
