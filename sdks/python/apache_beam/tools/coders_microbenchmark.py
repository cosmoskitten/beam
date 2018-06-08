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
to collect a per-element performance of various coders.

Run as
  python -m apache_beam.tools.coders_microbenchmark
"""

from __future__ import absolute_import
from __future__ import print_function

import collections
import random
import string
import sys

from apache_beam.coders import coders
from apache_beam.tools import utils
from apache_beam.transforms import window
from apache_beam.utils import windowed_value


def coder_benchmark_factory(name, coder, generate_fn):
  def init(self, size):
    self.coder_ = coder
    self.data_ = generate_fn(size)

  def call(self):
    _ = self.coder_.decode(self.coder_.encode(self.data_))

  benchmark = type(name, (object,), {})
  benchmark.__init__ = init
  benchmark.__call__ = call
  return benchmark


def generate_list_of_ints(input_size, lower_bound=0, upper_bound=sys.maxsize):
  values = []
  for _ in range(input_size):
    values.append(random.randint(lower_bound, upper_bound))
  return values


def generate_tuple_of_ints(input_size):
  return tuple(generate_list_of_ints(input_size))


def generate_string(length):
  return unicode(''.join(random.choice(
      string.ascii_letters + string.digits) for _ in range(length)))


def generate_dict_int_int(input_size):
  sample_list = generate_list_of_ints(input_size)
  return {val: val for val in sample_list}


def generate_dict_str_int(input_size):
  sample_list = generate_list_of_ints(input_size)
  return {generate_string(100): val for val in sample_list}


def generate_windowed_value_list(input_size):
  return [generate_windowed_int_value() for _ in range(0, input_size)]


def generate_windowed_int_value():
  return windowed_value.WindowedValue(
      value=random.randint(0, sys.maxsize),
      timestamp=12345678,
      windows=[
          window.IntervalWindow(50, 100),
          window.IntervalWindow(60, 110),
          window.IntervalWindow(70, 120),
      ])


def run_coder_benchmarks(num_runs, input_size, seed, verbose=True):
  random.seed(seed)

  # TODO(BEAM-4441): Pick coders using type hints, for example:
  # tuple_coder = typecoders.registry.get_coder(typehints.Tuple[int])
  benchmarks = [
      coder_benchmark_factory(
          "List[int], FastPrimitiveCoder", coders.FastPrimitivesCoder(),
          generate_list_of_ints),
      coder_benchmark_factory(
          "Tuple[int, int], FastPrimitiveCoder", coders.FastPrimitivesCoder(),
          generate_tuple_of_ints),
      coder_benchmark_factory(
          "Dict[int, int], FastPrimitiveCoder", coders.FastPrimitivesCoder(),
          generate_dict_int_int),
      coder_benchmark_factory(
          "Dict[str, int], FastPrimitiveCoder", coders.FastPrimitivesCoder(),
          generate_dict_str_int),
      coder_benchmark_factory(
          "List[int], IterableCoder+FastPrimitiveCoder",
          coders.IterableCoder(coders.FastPrimitivesCoder()),
          generate_list_of_ints),
      coder_benchmark_factory(
          "List[int WV], IterableCoder+WVCoder+FPCoder",
          coders.IterableCoder(coders.WindowedValueCoder(
              coders.FastPrimitivesCoder())),
          generate_windowed_value_list),
  ]

  BenchmarkConfig = collections.namedtuple(
      "BenchmarkConfig", ["benchmark", "size", "num_runs"])

  suite = [BenchmarkConfig(b, input_size, num_runs) for b in benchmarks]
  utils.run_benchmarks(suite, verbose=verbose)


if __name__ == "__main__":
  utils.check_compiled("apache_beam.coders.coder_impl")

  num_runs = 10
  input_size = 10000
  seed = 42 # Fix the seed for better consistency

  print("Number of runs:", num_runs)
  print("Input size:", input_size)
  print("Random seed:", seed)

  run_coder_benchmarks(num_runs, input_size, seed)
