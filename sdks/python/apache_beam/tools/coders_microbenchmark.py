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
"""A microbenchmark for measuring performance of coders.

This runs a sequence of encode-decode operations on random inputs
to collect performance of various coders.

To evaluate coders performance we approximate the behavior
how the coders are used in PCollections: we encode and decode
a (large) list of (small) collections. A collection can be a list of ints,
a tuple of windowed values, etc.

In this suite, an element of a benchmark is a collection with several entries.

Run as:
  python -m apache_beam.tools.coders_microbenchmark

"""

from __future__ import absolute_import
from __future__ import print_function

import math
import random
import string

from apache_beam.coders import coders
from apache_beam.tools import utils
from apache_beam.transforms import window
from apache_beam.utils import windowed_value

NUM_ENTRIES_PER_COLLECTION = 50


def coder_benchmark_factory(coder, generate_fn):
  """ Creates a benchmark that encodes and decodes a list of collections.

  Args:
    coder: coder to use to encode individual collections.
    generate_fn: a callable that generates a collection of a given size.
  """

  class CoderBenchmark(object):
    def __init__(self, num_collections_per_benchmark):
      self._coder = coders.IterableCoder(coder)
      self._list = [generate_fn(NUM_ENTRIES_PER_COLLECTION)
                    for _ in range(num_collections_per_benchmark)]

    def __call__(self):
      _ = self._coder.decode(self._coder.encode(self._list))

  CoderBenchmark.__name__ = "%s(%d entries), %s" % (
      generate_fn.__name__, NUM_ENTRIES_PER_COLLECTION, str(coder))

  return CoderBenchmark


def list_of_ints(size, upper_bound=1000000):
  """Generates a list of numbers with a distribution skewed to small values."""
  return [int(math.exp(random.random()**3*math.log(upper_bound)))
          for _ in range(size)]


def tuple_of_ints(size):
  return tuple(list_of_ints(size))


def random_string(length):
  return unicode(''.join(random.choice(
      string.ascii_letters + string.digits) for _ in range(length)))


def dict_int_int(size):
  return {i: i for i in list_of_ints(size)}


def dict_str_int(size):
  return {random_string(30): i for i in list_of_ints(size)}


def windowed_value_list(size):
  return [windowed_int_value() for _ in range(0, size)]


def windowed_int_value():
  return windowed_value.WindowedValue(
      value=list_of_ints(1)[0],
      timestamp=12345678,
      windows=(
          window.IntervalWindow(50, 100),
          window.IntervalWindow(60, 110),
          window.IntervalWindow(70, 120),
      ))


def run_coder_benchmarks(num_runs, input_size, seed, verbose):
  random.seed(seed)

  # TODO(BEAM-4441): Pick coders using type hints, for example:
  # tuple_coder = typecoders.registry.get_coder(typehints.Tuple[int, ...])
  benchmarks = [
      coder_benchmark_factory(
          coders.FastPrimitivesCoder(), list_of_ints),
      coder_benchmark_factory(
          coders.IterableCoder(coders.FastPrimitivesCoder()),
          list_of_ints),
      coder_benchmark_factory(
          coders.FastPrimitivesCoder(), tuple_of_ints),
      coder_benchmark_factory(
          coders.FastPrimitivesCoder(), dict_int_int),
      coder_benchmark_factory(
          coders.FastPrimitivesCoder(), dict_str_int),
      coder_benchmark_factory(
          coders.IterableCoder(coders.WindowedValueCoder(
              coders.FastPrimitivesCoder())),
          windowed_value_list),
  ]

  suite = [utils.BenchmarkConfig(b, input_size, num_runs) for b in benchmarks]
  utils.run_benchmarks(suite, verbose=verbose)


if __name__ == "__main__":
  utils.check_compiled("apache_beam.coders.coder_impl")

  num_runs = 20
  num_collections_per_benchmark = 1000
  seed = 42 # Fix the seed for better consistency

  run_coder_benchmarks(num_runs, num_collections_per_benchmark, seed,
                       verbose=True)
