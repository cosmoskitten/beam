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

import functools
import logging
import os
import tempfile
import time
import traceback
import unittest
from builtins import range

import apache_beam as beam
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.metricbase import MetricName
from apache_beam.runners.portability import fn_api_runner
from apache_beam.runners.worker import data_plane
from apache_beam.runners.worker import sdk_worker
from apache_beam.runners.worker import statesampler
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import userstate
from apache_beam.transforms import window

if statesampler.FAST_SAMPLER:
  DEFAULT_SAMPLING_PERIOD_MS = statesampler.DEFAULT_SAMPLING_PERIOD_MS
else:
  DEFAULT_SAMPLING_PERIOD_MS = 0


class FnApiRunnerTest(unittest.TestCase):

  def create_pipeline(self):
    return beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(use_grpc=False))

  def test_assert_that(self):
    # TODO: figure out a way for fn_api_runner to parse and raise the
    # underlying exception.
    with self.assertRaisesRegexp(Exception, 'Failed assert'):
      with self.create_pipeline() as p:
        assert_that(p | beam.Create(['a', 'b']), equal_to(['a']))

  def test_create(self):
    with self.create_pipeline() as p:
      assert_that(p | beam.Create(['a', 'b']), equal_to(['a', 'b']))

  def test_pardo(self):
    with self.create_pipeline() as p:
      res = (p
             | beam.Create(['a', 'bc'])
             | beam.Map(lambda e: e * 2)
             | beam.Map(lambda e: e + 'x'))
      assert_that(res, equal_to(['aax', 'bcbcx']))

  def test_pardo_metrics(self):

    class MyDoFn(beam.DoFn):

      def start_bundle(self):
        self.count = beam.metrics.Metrics.counter('ns1', 'elements')

      def process(self, element):
        self.count.inc(element)
        return [element]

    class MyOtherDoFn(beam.DoFn):

      def start_bundle(self):
        self.count = beam.metrics.Metrics.counter('ns2', 'elementsplusone')

      def process(self, element):
        self.count.inc(element + 1)
        return [element]

    with self.create_pipeline() as p:
      res = (p | beam.Create([1, 2, 3])
             | 'mydofn' >> beam.ParDo(MyDoFn())
             | 'myotherdofn' >> beam.ParDo(MyOtherDoFn()))
      p.run()
      if not MetricsEnvironment.METRICS_SUPPORTED:
        self.skipTest('Metrics are not supported.')

      counter_updates = [{'key': key, 'value': val}
                         for container in p.runner.metrics_containers()
                         for key, val in
                         container.get_updates().counters.items()]
      counter_values = [update['value'] for update in counter_updates]
      counter_keys = [update['key'] for update in counter_updates]
      assert_that(res, equal_to([1, 2, 3]))
      self.assertEqual(counter_values, [6, 9])
      self.assertEqual(counter_keys, [
          MetricKey('mydofn',
                    MetricName('ns1', 'elements')),
          MetricKey('myotherdofn',
                    MetricName('ns2', 'elementsplusone'))])

  def test_pardo_side_outputs(self):
    def tee(elem, *tags):
      for tag in tags:
        if tag in elem:
          yield beam.pvalue.TaggedOutput(tag, elem)
    with self.create_pipeline() as p:
      xy = (p
            | 'Create' >> beam.Create(['x', 'y', 'xy'])
            | beam.FlatMap(tee, 'x', 'y').with_outputs())
      assert_that(xy.x, equal_to(['x', 'xy']), label='x')
      assert_that(xy.y, equal_to(['y', 'xy']), label='y')

  def test_pardo_side_and_main_outputs(self):
    def even_odd(elem):
      yield elem
      yield beam.pvalue.TaggedOutput('odd' if elem % 2 else 'even', elem)
    with self.create_pipeline() as p:
      ints = p | beam.Create([1, 2, 3])
      named = ints | 'named' >> beam.FlatMap(
          even_odd).with_outputs('even', 'odd', main='all')
      assert_that(named.all, equal_to([1, 2, 3]), label='named.all')
      assert_that(named.even, equal_to([2]), label='named.even')
      assert_that(named.odd, equal_to([1, 3]), label='named.odd')

      unnamed = ints | 'unnamed' >> beam.FlatMap(even_odd).with_outputs()
      unnamed[None] | beam.Map(id)  # pylint: disable=expression-not-assigned
      assert_that(unnamed[None], equal_to([1, 2, 3]), label='unnamed.all')
      assert_that(unnamed.even, equal_to([2]), label='unnamed.even')
      assert_that(unnamed.odd, equal_to([1, 3]), label='unnamed.odd')

  def test_pardo_side_inputs(self):
    def cross_product(elem, sides):
      for side in sides:
        yield elem, side
    with self.create_pipeline() as p:
      main = p | 'main' >> beam.Create(['a', 'b', 'c'])
      side = p | 'side' >> beam.Create(['x', 'y'])
      assert_that(main | beam.FlatMap(cross_product, beam.pvalue.AsList(side)),
                  equal_to([('a', 'x'), ('b', 'x'), ('c', 'x'),
                            ('a', 'y'), ('b', 'y'), ('c', 'y')]))

  def test_pardo_windowed_side_inputs(self):
    with self.create_pipeline() as p:
      # Now with some windowing.
      pcoll = p | beam.Create(list(range(10))) | beam.Map(
          lambda t: window.TimestampedValue(t, t))
      # Intentionally choosing non-aligned windows to highlight the transition.
      main = pcoll | 'WindowMain' >> beam.WindowInto(window.FixedWindows(5))
      side = pcoll | 'WindowSide' >> beam.WindowInto(window.FixedWindows(7))
      res = main | beam.Map(lambda x, s: (x, sorted(s)),
                            beam.pvalue.AsList(side))
      assert_that(
          res,
          equal_to([
              # The window [0, 5) maps to the window [0, 7).
              (0, list(range(7))),
              (1, list(range(7))),
              (2, list(range(7))),
              (3, list(range(7))),
              (4, list(range(7))),
              # The window [5, 10) maps to the window [7, 14).
              (5, list(range(7, 10))),
              (6, list(range(7, 10))),
              (7, list(range(7, 10))),
              (8, list(range(7, 10))),
              (9, list(range(7, 10)))]),
          label='windowed')

  def test_flattened_side_input(self):
    with self.create_pipeline() as p:
      main = p | 'main' >> beam.Create([None])
      side1 = p | 'side1' >> beam.Create([('a', 1)])
      side2 = p | 'side2' >> beam.Create([('b', 2)])
      side = (side1, side2) | beam.Flatten()
      assert_that(
          main | beam.Map(lambda a, b: (a, b), beam.pvalue.AsDict(side)),
          equal_to([(None, {'a': 1, 'b': 2})]))

  def test_gbk_side_input(self):
    with self.create_pipeline() as p:
      main = p | 'main' >> beam.Create([None])
      side = p | 'side' >> beam.Create([('a', 1)]) | beam.GroupByKey()
      assert_that(
          main | beam.Map(lambda a, b: (a, b), beam.pvalue.AsDict(side)),
          equal_to([(None, {'a': [1]})]))

  def test_multimap_side_input(self):
    with self.create_pipeline() as p:
      main = p | 'main' >> beam.Create(['a', 'b'])
      side = (p | 'side' >> beam.Create([('a', 1), ('b', 2), ('a', 3)])
              # TODO(BEAM-4782): Obviate the need for this map.
              | beam.Map(lambda kv: (kv[0], kv[1])))
      assert_that(
          main | beam.Map(lambda k, d: (k, sorted(d[k])),
                          beam.pvalue.AsMultiMap(side)),
          equal_to([('a', [1, 3]), ('b', [2])]))

  def test_pardo_unfusable_side_inputs(self):
    def cross_product(elem, sides):
      for side in sides:
        yield elem, side
    with self.create_pipeline() as p:
      pcoll = p | beam.Create(['a', 'b'])
      assert_that(
          pcoll | beam.FlatMap(cross_product, beam.pvalue.AsList(pcoll)),
          equal_to([('a', 'a'), ('a', 'b'), ('b', 'a'), ('b', 'b')]))

    with self.create_pipeline() as p:
      pcoll = p | beam.Create(['a', 'b'])
      derived = ((pcoll,) | beam.Flatten()
                 | beam.Map(lambda x: (x, x))
                 | beam.GroupByKey()
                 | 'Unkey' >> beam.Map(lambda kv: kv[0]))
      assert_that(
          pcoll | beam.FlatMap(cross_product, beam.pvalue.AsList(derived)),
          equal_to([('a', 'a'), ('a', 'b'), ('b', 'a'), ('b', 'b')]))

  def test_pardo_state_only(self):
    p = self.create_pipeline()
    if not isinstance(p.runner, fn_api_runner.FnApiRunner):
      # test is inherited by Flink PVR, which does not support the feature yet
      self.skipTest('User state not supported.')

    index_state_spec = userstate.CombiningValueStateSpec(
        'index', beam.coders.VarIntCoder(), sum)

    # TODO(ccy): State isn't detected with Map/FlatMap.
    class AddIndex(beam.DoFn):
      def process(self, kv, index=beam.DoFn.StateParam(index_state_spec)):
        k, v = kv
        index.add(1)
        yield k, v, index.read()

    inputs = [('A', 'a')] * 2 + [('B', 'b')] * 3
    expected = [('A', 'a', 1),
                ('A', 'a', 2),
                ('B', 'b', 1),
                ('B', 'b', 2),
                ('B', 'b', 3)]

    with p:
      assert_that(p | beam.Create(inputs) | beam.ParDo(AddIndex()),
                  equal_to(expected))

  def test_group_by_key(self):
    with self.create_pipeline() as p:
      res = (p
             | beam.Create([('a', 1), ('a', 2), ('b', 3)])
             | beam.GroupByKey()
             | beam.Map(lambda k_vs: (k_vs[0], sorted(k_vs[1]))))
      assert_that(res, equal_to([('a', [1, 2]), ('b', [3])]))

  def test_flatten(self):
    with self.create_pipeline() as p:
      res = (p | 'a' >> beam.Create(['a']),
             p | 'bc' >> beam.Create(['b', 'c']),
             p | 'd' >> beam.Create(['d'])) | beam.Flatten()
      assert_that(res, equal_to(['a', 'b', 'c', 'd']))

  def test_combine_per_key(self):
    with self.create_pipeline() as p:
      res = (p
             | beam.Create([('a', 1), ('a', 2), ('b', 3)])
             | beam.CombinePerKey(beam.combiners.MeanCombineFn()))
      assert_that(res, equal_to([('a', 1.5), ('b', 3.0)]))

  def test_read(self):
    # Can't use NamedTemporaryFile as a context
    # due to https://bugs.python.org/issue14243
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    try:
      temp_file.write('a\nb\nc')
      temp_file.close()
      with self.create_pipeline() as p:
        assert_that(p | beam.io.ReadFromText(temp_file.name),
                    equal_to(['a', 'b', 'c']))
    finally:
      os.unlink(temp_file.name)

  def test_windowing(self):
    with self.create_pipeline() as p:
      res = (p
             | beam.Create([1, 2, 100, 101, 102])
             | beam.Map(lambda t: window.TimestampedValue(('k', t), t))
             | beam.WindowInto(beam.transforms.window.Sessions(10))
             | beam.GroupByKey()
             | beam.Map(lambda k_vs1: (k_vs1[0], sorted(k_vs1[1]))))
      assert_that(res, equal_to([('k', [1, 2]), ('k', [100, 101, 102])]))

  def test_large_elements(self):
    with self.create_pipeline() as p:
      big = (p
             | beam.Create(['a', 'a', 'b'])
             | beam.Map(lambda x: (x, x * data_plane._DEFAULT_FLUSH_THRESHOLD)))

      side_input_res = (
          big
          | beam.Map(lambda x, side: (x[0], side.count(x[0])),
                     beam.pvalue.AsList(big | beam.Map(lambda x: x[0]))))
      assert_that(side_input_res,
                  equal_to([('a', 2), ('a', 2), ('b', 1)]), label='side')

      gbk_res = (
          big
          | beam.GroupByKey()
          | beam.Map(lambda x: x[0]))
      assert_that(gbk_res, equal_to(['a', 'b']), label='gbk')

  def test_error_message_includes_stage(self):
    with self.assertRaises(BaseException) as e_cm:
      with self.create_pipeline() as p:
        def raise_error(x):
          raise RuntimeError('x')
        # pylint: disable=expression-not-assigned
        (p
         | beam.Create(['a', 'b'])
         | 'StageA' >> beam.Map(lambda x: x)
         | 'StageB' >> beam.Map(lambda x: x)
         | 'StageC' >> beam.Map(raise_error)
         | 'StageD' >> beam.Map(lambda x: x))
    self.assertIn('StageC', e_cm.exception.args[0])
    self.assertNotIn('StageB', e_cm.exception.args[0])

  def test_error_traceback_includes_user_code(self):

    def first(x):
      return second(x)

    def second(x):
      return third(x)

    def third(x):
      raise ValueError('x')

    try:
      with self.create_pipeline() as p:
        p | beam.Create([0]) | beam.Map(first)  # pylint: disable=expression-not-assigned
    except Exception:  # pylint: disable=broad-except
      message = traceback.format_exc()
    else:
      raise AssertionError('expected exception not raised')

    self.assertIn('first', message)
    self.assertIn('second', message)
    self.assertIn('third', message)

  def test_no_subtransform_composite(self):

    class First(beam.PTransform):
      def expand(self, pcolls):
        return pcolls[0]

    with self.create_pipeline() as p:
      pcoll_a = p | 'a' >> beam.Create(['a'])
      pcoll_b = p | 'b' >> beam.Create(['b'])
      assert_that((pcoll_a, pcoll_b) | First(), equal_to(['a']))

  def test_metrics(self):
    p = self.create_pipeline()
    if not isinstance(p.runner, fn_api_runner.FnApiRunner):
      # This test is inherited by others that may not support the same
      # internal way of accessing progress metrics.
      self.skipTest('Metrics not supported.')

    counter = beam.metrics.Metrics.counter('ns', 'counter')
    distribution = beam.metrics.Metrics.distribution('ns', 'distribution')
    gauge = beam.metrics.Metrics.gauge('ns', 'gauge')

    pcoll = p | beam.Create(['a', 'zzz'])
    # pylint: disable=expression-not-assigned
    pcoll | 'count1' >> beam.FlatMap(lambda x: counter.inc())
    pcoll | 'count2' >> beam.FlatMap(lambda x: counter.inc(len(x)))
    pcoll | 'dist' >> beam.FlatMap(lambda x: distribution.update(len(x)))
    pcoll | 'gauge' >> beam.FlatMap(lambda x: gauge.set(len(x)))

    res = p.run()
    res.wait_until_finish()
    c1, = res.metrics().query(beam.metrics.MetricsFilter().with_step('count1'))[
        'counters']
    self.assertEqual(c1.committed, 2)
    c2, = res.metrics().query(beam.metrics.MetricsFilter().with_step('count2'))[
        'counters']
    self.assertEqual(c2.committed, 4)
    dist, = res.metrics().query(beam.metrics.MetricsFilter().with_step('dist'))[
        'distributions']
    gaug, = res.metrics().query(
        beam.metrics.MetricsFilter().with_step('gauge'))['gauges']
    self.assertEqual(
        dist.committed.data, beam.metrics.cells.DistributionData(4, 2, 1, 3))
    self.assertEqual(dist.committed.mean, 2.0)
    self.assertEqual(gaug.committed.value, 3)

  def test_non_user_metrics(self):
    p = self.create_pipeline()
    if not isinstance(p.runner, fn_api_runner.FnApiRunner):
      # This test is inherited by others that may not support the same
      # internal way of accessing progress metrics.
      self.skipTest('Metrics not supported.')

    pcoll = p | beam.Create(['a', 'zzz'])
    # pylint: disable=expression-not-assigned
    pcoll | 'MyStep' >> beam.FlatMap(lambda x: None)
    res = p.run()
    res.wait_until_finish()

    all_metrics_via_montoring_infos = res.metrics(
        user_metrics_only=False).query()

    def assert_counter_exists(metrics, namespace, name, step):
      found = 0
      metric_key = MetricKey(step, MetricName(namespace, name))
      for m in metrics['counters']:
        if m.key == metric_key:
          found = found + 1
      self.assertEqual(
          1, found, "Did not find exactly 1 metric for %s." % metric_key)
    urns = [
        monitoring_infos.ELEMENT_COUNT_URN,
        monitoring_infos.START_BUNDLE_MSECS_URN,
        monitoring_infos.PROCESS_BUNDLE_MSECS_URN,
        monitoring_infos.FINISH_BUNDLE_MSECS_URN,
        monitoring_infos.TOTAL_MSECS_URN,
    ]
    for urn in urns:
      split = urn.split(':')
      namespace = split[0]
      name = ':'.join(split[1:])
      assert_counter_exists(
          all_metrics_via_montoring_infos, namespace, name, step='Create/Read')
      assert_counter_exists(
          all_metrics_via_montoring_infos, namespace, name, step='MyStep')

  def test_progress_metrics(self):
    logging.getLogger().setLevel(logging.INFO)
    p = self.create_pipeline()
    if not isinstance(p.runner, fn_api_runner.FnApiRunner):
      # This test is inherited by others that may not support the same
      # internal way of accessing progress metrics.
      self.skipTest('Progress metrics not supported.')

    _ = (p
         | beam.Create([0, 0, 0, 5e-3 * DEFAULT_SAMPLING_PERIOD_MS])
         | beam.Map(time.sleep)
         | beam.Map(lambda x: ('key', x))
         | beam.GroupByKey()
         | 'm_out' >> beam.FlatMap(lambda x: [
             1, 2, 3, 4, 5,
             beam.pvalue.TaggedOutput('once', x),
             beam.pvalue.TaggedOutput('twice', x),
             beam.pvalue.TaggedOutput('twice', x)]))
    res = p.run()
    res.wait_until_finish()

    try:
      self.assertEqual(2, len(res._monitoring_infos_by_stage))
      pregbk_mis, postgbk_mis = list(res._monitoring_infos_by_stage.values())
      if 'Create/Read' not in pregbk_mis[0].labels['PTRANSFORM']:
        # The monitoring infos above are actually unordered. Swap.
        pregbk_mis, postgbk_mis = postgbk_mis, pregbk_mis

      def assert_has_monitoring_info(monitoring_infos, urn, value, labels):
        found = 0
        for m in monitoring_infos:
          if (m.labels == labels and
              m.metric.counter_data.int64_value == value and
              m.urn == urn):
            found = found + 1
        self.assertEqual(
            1, found, "Found (%s) Expected only 1 monitoring_info for %s." %
            (found, (urn, value, labels),))

      # pregbk_mis
      labels = {'PTRANSFORM' : 'Create/Read', 'TAG' : 'None'}
      assert_has_monitoring_info(
          pregbk_mis, monitoring_infos.ELEMENT_COUNT_URN, 4, labels)
      labels = {'PTRANSFORM' : 'Map(sleep)'}
      assert_has_monitoring_info(
          pregbk_mis, monitoring_infos.TOTAL_MSECS_URN,
          4e-3 * DEFAULT_SAMPLING_PERIOD_MS, labels)

      # postgbk_metrics
      labels = {'PTRANSFORM' : 'GroupByKey/Read', 'TAG' : 'None'}
      assert_has_monitoring_info(
          postgbk_mis, monitoring_infos.ELEMENT_COUNT_URN, 1, labels)
      labels = {'PTRANSFORM' : 'm_out', 'TAG' : 'None'}
      assert_has_monitoring_info(
          postgbk_mis, monitoring_infos.ELEMENT_COUNT_URN, 5, labels)
      labels = {'PTRANSFORM' : 'm_out', 'TAG' : 'once'}
      assert_has_monitoring_info(
          postgbk_mis, monitoring_infos.ELEMENT_COUNT_URN, 1, labels)
      labels = {'PTRANSFORM' : 'm_out', 'TAG' : 'twice'}
      assert_has_monitoring_info(
          postgbk_mis, monitoring_infos.ELEMENT_COUNT_URN, 2, labels)
    except:
      print(res._monitoring_infos_by_stage)
      raise


class FnApiRunnerTestWithGrpc(FnApiRunnerTest):

  def create_pipeline(self):
    return beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(use_grpc=True))


class FnApiRunnerTestWithGrpcMultiThreaded(FnApiRunnerTest):

  def create_pipeline(self):
    return beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(
            use_grpc=True,
            sdk_harness_factory=functools.partial(
                sdk_worker.SdkHarness, worker_count=2)))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
