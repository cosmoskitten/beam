#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
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

"""Utility functions for all microbenchmarks."""

from __future__ import absolute_import
from __future__ import print_function

import collections
import gc
import os
import time

from numpy import median


def check_compiled(module):
  """Check whether given module has been compiled.
  Args:
    module: string, module name
  """
  check_module = __import__(module, globals(), locals(), -1)
  ext = os.path.splitext(check_module.__file__)[-1]
  if ext in ('.py', '.pyc'):
    raise RuntimeError(
        "Profiling uncompiled code.\n"
        "To compile beam, run "
        "'pip install Cython; python setup.py build_ext --inplace'")


def run_benchmarks(benchmarks, num_runs, print_output):
  """Runs benchmarks, and collects execution times.

  A simple instrumentation to run any python function several times
  on a generated input, collect and print its execution times.

  Args:
    benchmarks: A list of dictionaries that describe benchmarks.
      Each dictionary entry should have following key-value pairs:
      'name': str, name of the benchmark.
      'input_generator': a function that takes no arguments that creates an
         input for a benchmark.
      'input_size': int, a size of the input. Aggregated per-element metrics
         are counted based on the size of the input.
      'op_fn': a function that takes one argument, that will be called on the
         generated input.
    num_runs: int, number of times to run each benchmark.
    print_output: Whether to print benchmark results to stdout.

  Returns:
    A dictionary of the form str: list of floats. Keys of the dictionary
    are benchmark names, values are execution times in seconds for each run
    divided by benchmark input size.
  """

  def run(benchmark):
    # Contain each run of a benchmark inside a function so that any temporary
    # objects can be garbage-collected after the run.
    input_ = benchmark["input_generator"]()
    op = benchmark["op_fn"]
    start = time.time()
    _ = op(input_)
    return time.time() - start

  if print_output:
    pad_length = max([len(b["name"]) for b in benchmarks])

  cost_series = collections.defaultdict(list)
  for run_id in range(num_runs):
    for b in benchmarks:
      # Do a proactive GC before each run to minimize side-effects of different
      # runs.
      gc.collect()
      time_cost = run(b)
      cost_series[b["name"]].append(time_cost / b["input_size"])

    if print_output:
      print("Run %d of %d:" % (run_id+1, num_runs))
      for benchmark_name in sorted(cost_series):
        avg_cost = cost_series[benchmark_name][run_id]
        print("%s: per element time cost: %g sec" % (
            benchmark_name.ljust(pad_length, " "), avg_cost))
      print("")

  if print_output:
    print("Median time cost:")
    for benchmark_name in sorted(cost_series):
      median_cost = median(cost_series[benchmark_name])
      print("%s: per element median time cost: %g sec" % (
          benchmark_name.ljust(pad_length, " "), median_cost))

  return cost_series
