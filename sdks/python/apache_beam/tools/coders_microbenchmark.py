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

import random
import string
import sys

from apache_beam.coders import coders
from apache_beam.tools import utils
from apache_beam.transforms import window
from apache_beam.utils import windowed_value


def generate_list_of_ints(input_size, lower_bound=0, upper_bound=sys.maxsize):
  values = []
  for _ in range(input_size):
    values.append(random.randint(lower_bound, upper_bound))
  return values


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


def encode_and_decode(coder, data):
  _ = coder.decode(coder.encode(data))


def run_coder_benchmarks(num_runs, input_size, seed, print_output=True):
  random.seed(seed)

  benchmarks = []

  # TODO(BEAM-4441): Pick coders using type hints, for example:
  # tuple_coder = typecoders.registry.get_coder(typehints.Tuple[int])

  benchmarks.append({
      "name": "List[int], FastPrimitiveCoder",
      "input_generator": lambda: generate_list_of_ints(input_size),
      "op_fn": lambda input_: encode_and_decode(
          coders.FastPrimitivesCoder(), input_),
      "input_size": input_size
  })

  benchmarks.append({
      "name": "Dict[str, int], FastPrimitiveCoder",
      "input_generator": lambda: generate_dict_str_int(input_size),
      "op_fn": lambda input_: encode_and_decode(
          coders.FastPrimitivesCoder(), input_),
      "input_size": input_size
  })

  benchmarks.append({
      "name": "Dict[int, int], FastPrimitiveCoder",
      "input_generator": lambda: generate_dict_int_int(input_size),
      "op_fn": lambda input_: encode_and_decode(
          coders.FastPrimitivesCoder(), input_),
      "input_size": input_size
  })

  benchmarks.append({
      "name": "Tuple[int], FastPrimitiveCoder",
      "input_generator": lambda: tuple(generate_list_of_ints(input_size)),
      "op_fn": lambda input_: encode_and_decode(
          coders.FastPrimitivesCoder(), input_),
      "input_size": input_size
  })

  benchmarks.append({
      "name": "List[int WV], IterableCoder+WVCoder+FPCoder",
      "input_generator": lambda: generate_windowed_value_list(input_size),
      "op_fn": lambda input_: encode_and_decode(coders.IterableCoder(
          coders.WindowedValueCoder(coders.FastPrimitivesCoder())), input_),
      "input_size": input_size
  })

  benchmarks.append({
      "name": "List[int], IterableCoder+FastPrimitiveCoder",
      "input_generator": lambda: generate_list_of_ints(input_size),
      "op_fn": lambda input_: encode_and_decode(
          coders.IterableCoder(coders.FastPrimitivesCoder()), input_),
      "input_size": input_size
  })

  utils.run_benchmarks(benchmarks, num_runs, print_output=print_output)


if __name__ == "__main__":
  utils.check_compiled("apache_beam.coders.coder_impl")

  num_runs = 10
  input_size = 10000
  seed = 42 # Fix the seed for better consistency

  print("Number of runs:", num_runs)
  print("Input size:", input_size)
  print("Random seed:", seed)

  run_coder_benchmarks(num_runs, input_size, seed)
