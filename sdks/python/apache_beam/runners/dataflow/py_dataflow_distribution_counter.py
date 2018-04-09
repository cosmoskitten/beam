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

""" For internal use only. No backwards compatibility guarantees."""

from apache_beam.transforms.cy_combiners import AccumulatorCombineFn

class DataflowDistributionCounter(object):
  """Pure python DistributionAccumulator in case Cython not available
  Pure python DistributionAccumulator will no nothing since it's super slow
  """
  def __init__(self):
    self.min = 0
    self.max = 0
    self.count = 0
    self.sum = 0
    self.first_bucket_offset = 0
    self.buckets = []
    self.bucket_per_10 = 3

  def add_input(self, element):
    pass

  def get_add_input_fn(self):
    return self.add_input

  def translate_to_histogram(self, histogram):
    pass


class DataflowDistributionCounterFn(AccumulatorCombineFn):
  """A subclass of cy_combiners.AccumulatorCombineFn
  Make DataflowDistributionCounter able to report to Dataflow service via
  CounterFactory
  When cythonized DataflowDistributinoCounter available, make
  CounterFn combine with cythonized module, otherwise, combine with python
  version
  """
  try:
    from apache_beam.runners.dataflow.cy_dataflow_distribution_counter \
      import DataflowDistributionCounter
    _accumulator_type = DataflowDistributionCounter
  except ImportError:
    _accumulator_type = DataflowDistributionCounter
