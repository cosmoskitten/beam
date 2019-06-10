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


def pardo_simple(test=None):
  # [START pardo_simple]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            'Strawberry Carrot Artichoke',
            'Tomato Potato',
        ])
        | 'Split words' >> beam.ParDo(str.split)
        | beam.Map(print)
    )
    # [END pardo_simple]
    if test:
      test(plants)


def pardo_function(test=None):
  # [START pardo_function]
  import apache_beam as beam

  def split_words(text):
    return text.split(',')

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            'Strawberry,Carrot,Artichoke',
            'Tomato,Potato',
        ])
        | 'Split words' >> beam.ParDo(split_words)
        | beam.Map(print)
    )
    # [END pardo_function]
    if test:
      test(plants)


def pardo_lambda(test=None):
  # [START pardo_lambda]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            ['Strawberry', 'Carrot', 'Artichoke'],
            ['Tomato', 'Potato'],
        ])
        | 'Flatten lists' >> beam.ParDo(lambda elements: elements)
        | beam.Map(print)
    )
    # [END pardo_lambda]
    if test:
      test(plants)


def pardo_generator(test=None):
  # [START pardo_generator]
  import apache_beam as beam

  def list_elements(elements):
    for element in elements:
      yield element

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            ['Strawberry', 'Carrot', 'Artichoke'],
            ['Tomato', 'Potato'],
        ])
        | 'Flatten lists' >> beam.ParDo(list_elements)
        | beam.Map(print)
    )
    # [END pardo_generator]
    if test:
      test(plants)


def pardo_multiple_arguments(test=None):
  # [START pardo_multiple_arguments]
  import apache_beam as beam

  def split_words(text, delimiter=None):
    return text.split(delimiter)

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            'Strawberry,Carrot,Artichoke',
            'Tomato,Potato',
        ])
        | 'Split words' >> beam.ParDo(split_words, delimiter=',')
        | beam.Map(print)
    )
    # [END pardo_multiple_arguments]
    if test:
      test(plants)


def pardo_dofn(test=None):
  # [START pardo_dofn]
  import apache_beam as beam

  class SplitWords(beam.DoFn):
    def __init__(self, delimiter=','):
      self.delimiter = delimiter

    def process(self, text):
      return text.split(self.delimiter)

  with beam.Pipeline() as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            'Strawberry,Carrot,Artichoke',
            'Tomato,Potato',
        ])
        | 'Split words' >> beam.ParDo(SplitWords(','))
        | beam.Map(print)
    )
    # [END pardo_dofn]
    if test:
      test(plants)


def pardo_dofn_params(test=None):
  # [START pardo_dofn_params]
  import apache_beam as beam

  class AnalyzeElement(beam.DoFn):
    def process(self, elem, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
      yield '\n'.join([
          '# timestamp',
          'type(timestamp) -> ' + repr(type(timestamp)),
          'timestamp.micros -> ' + repr(timestamp.micros),
          'timestamp.to_rfc3339() -> ' + repr(timestamp.to_rfc3339()),
          'timestamp.to_utc_datetime() -> ' + repr(timestamp.to_utc_datetime()),
          '',
          '# window',
          'type(window) -> ' + repr(type(window)),
          'window.start -> {} ({})'.format(window.start, window.start.to_utc_datetime()),
          'window.end -> {} ({})'.format(window.end, window.end.to_utc_datetime()),
          'window.max_timestamp() -> {} ({})'.format(window.max_timestamp(), window.max_timestamp().to_utc_datetime()),
      ])

  with beam.Pipeline() as pipeline:
    dofn_params = (
        pipeline
        | 'Create a single test element' >> beam.Create([':)'])
        | 'Add timestamp (Spring equinox 2020)' >> beam.Map(
            lambda elem: beam.window.TimestampedValue(elem, 1584675660))
        | 'Fixed 30sec windows' >> beam.WindowInto(beam.window.FixedWindows(30))
        | 'Analyze element' >> beam.ParDo(AnalyzeElement())
        | beam.Map(print)
    )
    # [END pardo_dofn_params]
    if test:
      test(dofn_params)


def pardo_side_inputs_singleton(test=None):
  # [START pardo_side_inputs_singleton]
  import apache_beam as beam

  with beam.Pipeline() as pipeline:
    delimiter = pipeline | 'Create delimiter' >> beam.Create([','])

    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            'Strawberry,Carrot,Artichoke',
            'Tomato,Potato',
        ])
        | 'Split words' >> beam.ParDo(
            lambda text, delimiter: text.split(delimiter),
            delimiter=beam.pvalue.AsSingleton(delimiter),
        )
        | beam.Map(print)
    )
    # [END pardo_side_inputs_singleton]
    if test:
      test(plants)


def pardo_side_inputs_iter(test=None):
  # [START pardo_side_inputs_iter]
  import apache_beam as beam

  def validate_durations(plant, valid_durations):
    if plant['duration'] in valid_durations:
      yield plant

  with beam.Pipeline() as pipeline:
    valid_durations = pipeline | 'Valid durations' >> beam.Create([
        'annual',
        'biennial',
        'perennial',
    ])

    valid_plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {'name': 'Strawberry', 'duration': 'perennial'},
            {'name': 'Carrot', 'duration': 'biennial'},
            {'name': 'Artichoke', 'duration': 'perennial'},
            {'name': 'Tomato', 'duration': 'annual'},
            {'name': 'Potato', 'duration': 'unknown'},  # it's perennial ;)
        ])
        | 'Validate durations' >> beam.ParDo(
            validate_durations,
            valid_durations=beam.pvalue.AsIter(valid_durations),
        )
        | beam.Map(print)
    )
    # [END pardo_side_inputs_iter]
    if test:
      test(valid_plants)


def pardo_side_inputs_dict(test=None):
  # [START pardo_side_inputs_dict]
  import apache_beam as beam

  def replace_duration_if_valid(plant, durations):
    if plant['duration'] in durations:
      plant['duration'] = durations[plant['duration']]
      yield plant

  with beam.Pipeline() as pipeline:
    durations = pipeline | 'Durations dict' >> beam.Create([
        (0, 'annual'),
        (1, 'biennial'),
        (2, 'perennial'),
    ])

    valid_plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            {'name': 'Strawberry', 'duration': 2},
            {'name': 'Carrot', 'duration': 1},
            {'name': 'Artichoke', 'duration': 2},
            {'name': 'Tomato', 'duration': 0},
            {'name': 'Potato', 'duration': -1},
        ])
        | 'Replace duration if valid' >> beam.ParDo(
            replace_duration_if_valid,
            durations=beam.pvalue.AsDict(durations),
        )
        | beam.Map(print)
    )
    # [END pardo_side_inputs_dict]
    if test:
      test(valid_plants)


if __name__ == '__main__':
  eval(util.parse_example())
