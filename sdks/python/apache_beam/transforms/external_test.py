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

"""Unit tests for the transform.util classes."""

from __future__ import absolute_import

import unittest

import apache_beam as beam
from apache_beam.runners.portability import construction_service
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import ptransform


class ExternalTransformTest(unittest.TestCase):

  def test_simple(self):

    @ptransform.PTransform.register_urn("simple", None)
    class SimpleTransform(ptransform.PTransform):
      def expand(self, pcoll):
        return pcoll | beam.Map(lambda x: 'Simple(%s)' % x)

      def to_runner_api_parameter(self, unused_context):
        return 'simple', None

      @staticmethod
      def from_runner_api_parameter(unused_parameter, unused_context):
        return SimpleTransform()

    with beam.Pipeline() as p:
      res = (
          p
          | beam.Create(['a', 'b'])
          | beam.ExternalTransform(
              'simple',
              None,
              construction_service.ConstructionServiceServicer()))
      assert_that(res, equal_to(['Simple(a)', 'Simple(b)']))

  def test_multi(self):

    @ptransform.PTransform.register_urn("multi", None)
    class MutltiTransform(ptransform.PTransform):
      def expand(self, pcolls):
        return {
            'main':
                (pcolls['main1'], pcolls['main2'])
                | beam.Flatten()
                | beam.Map(lambda x, s: x + s,
                           beam.pvalue.AsSingleton(pcolls['side'])),
            'side': pcolls['side'] | beam.Map(lambda x: x + x),
        }

      def to_runner_api_parameter(self, unused_context):
        return 'multi', None

      @staticmethod
      def from_runner_api_parameter(unused_parameter, unused_context):
        return MutltiTransform()

    with beam.Pipeline() as p:
      main1 = p | 'Main1' >> beam.Create(['a', 'bb'], reshuffle=False)
      main2 = p | 'Main2' >> beam.Create(['x', 'yy', 'zzz'], reshuffle=False)
      side = p | 'Side' >> beam.Create(['s'])
      res = dict(main1=main1, main2=main2, side=side) | beam.ExternalTransform(
          'multi', None, construction_service.ConstructionServiceServicer())
      assert_that(res['main'], equal_to(['as', 'bbs', 'xs', 'yys', 'zzzs']))
      assert_that(res['side'], equal_to(['ss']), label='CheckSide')

  def test_payload(self):

    @ptransform.PTransform.register_urn("payload", bytes)
    class PayloadTransform(ptransform.PTransform):
      def __init__(self, payload):
        self._payload = payload

      def expand(self, pcoll):
        return pcoll | beam.Map(lambda x, s: x + s, self._payload)

      def to_runner_api_parameter(self, unused_context):
        return 'payload', self._payload

      @staticmethod
      def from_runner_api_parameter(payload, unused_context):
        return PayloadTransform(payload)

    with beam.Pipeline() as p:
      res = (
          p
          | beam.Create(['a', 'bb'], reshuffle=False)
          | beam.ExternalTransform(
              'payload', 's',
              construction_service.ConstructionServiceServicer()))
      assert_that(res, equal_to(['as', 'bbs']))

  def test_nested(self):
    @ptransform.PTransform.register_urn("fib", bytes)
    class FibTransform(ptransform.PTransform):
      def __init__(self, level):
        self._level = level

      def expand(self, p):
        if self._level <= 2:
          return p | beam.Create([1])
        else:
          a = p | 'A' >> beam.ExternalTransform(
              'fib', bytes(self._level - 1),
              construction_service.ConstructionServiceServicer())
          b = p | 'B' >> beam.ExternalTransform(
              'fib', bytes(self._level - 2),
              construction_service.ConstructionServiceServicer())
          return (
              (a, b)
              | beam.Flatten()
              | beam.CombineGlobally(sum).without_defaults())

      def to_runner_api_parameter(self, unused_context):
        return 'fib', bytes(self._level)

      @staticmethod
      def from_runner_api_parameter(level, unused_context):
        return FibTransform(int(level))

    with beam.Pipeline() as p:
      assert_that(p | FibTransform(6), equal_to([8]))
