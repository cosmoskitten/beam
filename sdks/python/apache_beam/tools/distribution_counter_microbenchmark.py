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
"""A microbenchmark for measuring DistributionAccumulator performance

This runs a sequence of distribution.update for random input value to calculate
average update time per input.
A typical update operation should run into 0.6 microseconds

Run as
  python -m apache_beam.tools.map.distribution_counter_microbenchmark
"""

from __future__ import print_function

import random
import time

from apache_beam.transforms.cy_combiners import DistributionAccumulator
from apache_beam.tools import utils

def run_benchmark(num_runs=100, num_input=10000):
  total_time = 0
  # pylint: disable=unused-variable
  for i in range(num_runs):
    random.seed(time.time())
    counter = DistributionAccumulator()
    time_cost = 0
    # pylint: disable=unused-variable
    for j in range(num_input):
      value = random.randint(1, 10000)
      start = time.time()
      counter.add_input(value)
      time_cost += (time.time() - start)
    total_time += (time_cost/num_input)
  print("Per element update time cost: ", total_time/num_runs)

if __name__ == '__main__':
  utils.check_compiled('cy_combiners')
  run_benchmark()
