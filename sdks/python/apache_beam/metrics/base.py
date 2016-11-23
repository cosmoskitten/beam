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

"""

Available classes:
- Metric - Base class of a metrics object. Provides very basic methods.
- Counter - Base class of a counter metric object. It provides methods to
    increment/decrement a count variable accross a pipeline execution.
- Distribution - Base class of a distribution metric object. It provides
    methods to keep track of statistics about the distribution of a variable.

Named tuples:
- MetricName - Base name of a metric: (namespace=str, name=str)
- MetricKey - Base internal key of a metric
    (step_name=str, metric=(namespace=str, name=str))
- MetricResult - Current status of a metric's updates/commits
    (metric=(namespace=str, name=str), step=str, committed=data, attempted=data)
"""
from collections import namedtuple


MetricName = namedtuple('MetricName', 'namespace name')
MetricKey = namedtuple('MetricKey', 'step metric')
MetricResult = namedtuple('MetricResult', 'metric step committed attempted')


class Metric(object):
  """ Base class of a metric object.
  """


class Counter(Metric):
  """ Base class of a Counter metric object
  """
  def inc(self, n=1):
    raise NotImplementedError

  def dec(self, n=1):
    self.inc(-n)


class Distribution(Metric):
  """ Base class of a Distribution metric object
  """
  def update(self, value):
    raise NotImplementedError


class MetricsUpdates(object):
  """ Simple class that contains metrics updates.

  A metric update is an object containing information to update a metric.
  For Distribution metrics, it is DistributionData, and for Counter metrics,
  it's an int.
  """
  def __init__(self, counters=None, distributions=None):
    """ Create a MetricUpdates object

    Args:
      counters - Dictionary of MetricKey:MetricUpdate updates.
      distributions - Dictionary of MetricKey:MetricUpdate objects.
    """
    self.counters = counters or {}
    self.distributions = distributions or {}
