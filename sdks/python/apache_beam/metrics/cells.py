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

import threading

from apache_beam.metrics.base import Counter
from apache_beam.metrics.base import Distribution


class CellCommitState(object):
  """Keeps track of a cell's commit status.

  It's thread-safe.
  """
  DIRTY = 0
  CLEAN = 1
  COMMITTING = 2

  def __init__(self):
    self._lock = threading.Lock()
    self._state = CellCommitState.DIRTY

  @property
  def state(self):
    return self._state

  def modified(self):
    with self._lock:
      self._state = CellCommitState.DIRTY

  def after_commit(self):
    with self._lock:
      if self._state == CellCommitState.COMMITTING:
        self._state = CellCommitState.CLEAN

  def before_commit(self):
    with self._lock:
      if self._state == CellCommitState.CLEAN:
        return False
      else:
        self._state = CellCommitState.COMMITTING
        return True


class MetricCell(object):
  """Base class of Cell that tracks state of a metric during pipeline execution.

  All subclasses must be thread safe, as these are used in the
  pipeline runners, and may be subject to parallel/concurrent
  updates.
  """
  def __init__(self):
    self.commit = CellCommitState()
    self._lock = threading.Lock()

  def get_cumulative(self):
    raise NotImplementedError


class CounterCell(Counter, MetricCell):
  """Keeps the state of a counter metric during pipeline execution.

  It's thread safe.
  """
  def __init__(self, *args):
    super(CounterCell, self).__init__(*args)
    self.value = 0

  def combine(self, other):
    result = CounterCell()
    result.inc(self.value + other.value)
    return result

  def inc(self, n=1):
    with self._lock:
      self.value += n
      self.commit.modified()

  def get_cumulative(self):
    with self._lock:
      return self.value


class DistributionCell(Distribution, MetricCell):
  """Keeps the state of a distribution metric during pipeline execution.

  It's thread safe.
  """
  def __init__(self, *args):
    super(DistributionCell, self).__init__(*args)
    self.data = DistributionData(0, 0, None, None)

  def combine(self, other):
    result = DistributionCell()
    result.data = self.data.combine(other.data)
    return result

  def update(self, value):
    with self._lock:
      self.commit.modified()
      self._update(value)

  def _update(self, value):
    self.data._count += 1
    self.data._sum += value
    self.data._min = (value
                      if self.data.min is None or self.data.min > value
                      else self.data.min)
    self.data._max = (value
                      if self.data.max is None or self.data.max < value
                      else self.data.max)

  def get_cumulative(self):
    with self._lock:
      return self.data.get_cumulative()


class DistributionData(object):
  """The data structure that holds data about a distribution metric.

  This object is not thread safe, so it's not supposed to be modified
  by other than the DistributionCell that contains it.
  """
  def __init__(self, sum, count, min, max):
    self._sum = sum
    self._count = count
    self._min = min
    self._max = max

  def __eq__(self, other):
    return (self.sum == other.sum and
            self.count == other.count and
            self.min == other.min and
            self.max == other.max)

  def __neq__(self, other):
    return not self.__eq__(other)

  def __repr__(self):
    return '<DistributionData({}, {}, {}, {})>'.format(self.sum,
                                                       self.count,
                                                       self.min,
                                                       self.max)

  def get_cumulative(self):
    return DistributionData(self.sum, self.count, self.min, self.max)

  @property
  def sum(self):
    return self._sum

  @property
  def count(self):
    return self._count

  @property
  def min(self):
    return self._min

  @property
  def max(self):
    return self._max

  @property
  def mean(self):
    return self.sum / self.count

  def combine(self, other):
    if other is None:
      return self
    else:
      new_min = (None if self.min is None and other.min is None else
                 min(x for x in (self.min, other.min) if x is not None))
      return DistributionData(
          self.sum + other.sum,
          self.count + other.count,
          new_min,
          max(self.max, other.max))

  @classmethod
  def singleton(cls, value):
    return DistributionData(value, 1, value, value)


class MetricAggregation(object):
  """Base class for aggregating metric data."""
  def combine(self, updates):
    raise NotImplementedError

  def zero(self):
    raise NotImplementedError


class CounterAggregator(object):
  """Class for aggregating data from Counter metrics.

  It works with pure integers.
  """
  def zero(self):
    return 0

  def combine(self, x, y):
    return x + y


class DistributionAggregator(object):
  """Class for aggregating data from Distribution metrics.

  It works with DistributionData objects.
  """
  def zero(self):
    return DistributionData(0, 0, None, None)

  def combine(self, x, y):
    return x.combine(y)
