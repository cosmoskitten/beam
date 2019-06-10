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

from __future__ import absolute_import
from __future__ import print_function

from apache_beam.examples.snippets import util


def partition_function(test=None):
  # [START partition_function]
  import apache_beam as beam

  durations = ['annual', 'biennial', 'perennial']

  def by_duration(plant, num_partitions):
    return durations.index(plant['duration'])

  with beam.Pipeline() as pipeline:
    annuals, biennials, perennials = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {'name': 'Strawberry', 'duration': 'perennial'},
            {'name': 'Carrot', 'duration': 'biennial'},
            {'name': 'Artichoke', 'duration': 'perennial'},
            {'name': 'Tomato', 'duration': 'annual'},
            {'name': 'Potato', 'duration': 'perennial'},
        ])
        | 'Partition' >> beam.Partition(by_duration, len(durations))
    )
    annuals    | 'Annuals'    >> beam.Map(lambda x: print('annual:    ' + str(x)))
    biennials  | 'Biennials'  >> beam.Map(lambda x: print('biennial:  ' + str(x)))
    perennials | 'Perennials' >> beam.Map(lambda x: print('perennial: ' + str(x)))
    # [END partition_function]
    if test:
      test(annuals, biennials, perennials)


def partition_lambda(test=None):
  # [START partition_lambda]
  import apache_beam as beam

  durations = ['annual', 'biennial', 'perennial']

  with beam.Pipeline() as pipeline:
    annuals, biennials, perennials = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {'name': 'Strawberry', 'duration': 'perennial'},
            {'name': 'Carrot', 'duration': 'biennial'},
            {'name': 'Artichoke', 'duration': 'perennial'},
            {'name': 'Tomato', 'duration': 'annual'},
            {'name': 'Potato', 'duration': 'perennial'},
        ])
        | 'Partition' >> beam.Partition(
            lambda plant, num_partitions: durations.index(plant['duration']),
            len(durations),
        )
    )
    annuals    | 'Annuals'    >> beam.Map(lambda x: print('annual:    ' + str(x)))
    biennials  | 'Biennials'  >> beam.Map(lambda x: print('biennial:  ' + str(x)))
    perennials | 'Perennials' >> beam.Map(lambda x: print('perennial: ' + str(x)))
    # [END partition_lambda]
    if test:
      test(annuals, biennials, perennials)


def partition_multiple_arguments(test=None):
  # [START partition_multiple_arguments]
  import apache_beam as beam
  from random import Random

  random = Random(0)
  with beam.Pipeline() as pipeline:
    train_dataset, test_dataset = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {'name': 'Strawberry', 'duration': 'perennial'},
            {'name': 'Carrot', 'duration': 'biennial'},
            {'name': 'Artichoke', 'duration': 'perennial'},
            {'name': 'Tomato', 'duration': 'annual'},
            {'name': 'Potato', 'duration': 'perennial'},
        ])
        | 'Partition' >> beam.Partition(
            lambda plant, n, train_ratio: int(random.random() > train_ratio),
            2, train_ratio=0.8,
        )
    )
    train_dataset | 'Train' >> beam.Map(lambda x: print('train: ' + str(x)))
    test_dataset  | 'Test'  >> beam.Map(lambda x: print('test:  ' + str(x)))
    # [END partition_multiple_arguments]
    if test:
      test(train_dataset, test_dataset)


if __name__ == '__main__':
  eval(util.parse_example())
