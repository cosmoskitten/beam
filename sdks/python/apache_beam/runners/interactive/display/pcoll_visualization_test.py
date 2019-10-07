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

"""Tests for apache_beam.runners.interactive.display.pcoll_visualization."""
from __future__ import absolute_import

import sys
import time
import timeloop
import unittest

import apache_beam as beam
from apache_beam.runners import runner
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive.display import pcoll_visualization as pv
from unittest.mock import patch


class PCollVisualizationTest(unittest.TestCase):

  def setUp(self):
    self._p = beam.Pipeline()
    # pylint: disable=range-builtin-not-iterating
    self._pcoll = self._p | 'Create' >> beam.Create(range(1000))

  @unittest.skipIf(sys.version_info < (3,),
                   'PCollVisualization is not supported on Python 2.')
  def test_raise_error_for_non_pcoll_input(self):
    class Foo(object):
      pass

    with self.assertRaises(ValueError) as ctx:
      pv.PCollVisualization(Foo())
      self.assertTrue('pcoll should be apache_beam.pvalue.PCollection' in
                      ctx.exception)

  @unittest.skipIf(sys.version_info < (3,),
                   'PCollVisualization is not supported on Python 2.')
  def test_pcoll_visualization_generate_unique_display_id(self):
    pv_1 = pv.PCollVisualization(self._pcoll)
    pv_2 = pv.PCollVisualization(self._pcoll)
    self.assertNotEqual(pv_1._dive_display_id, pv_2._dive_display_id)
    self.assertNotEqual(pv_1._overview_display_id, pv_2._overview_display_id)
    self.assertNotEqual(pv_1._df_display_id, pv_2._df_display_id)

  @unittest.skipIf(sys.version_info < (3,),
                   'PCollVisualization is not supported on Python 2.')
  @patch('apache_beam.runners.interactive.display.pcoll_visualization'
         '.PCollVisualization._to_element_list', lambda x: [1, 2, 3])
  def test_one_shot_visualization_not_return_handle(self):
    self.assertIsNone(pv.visualize(self._pcoll))

  def _mock_to_element_list(self):
    yield [1, 2, 3]
    yield [1, 2, 3, 4]

  @unittest.skipIf(sys.version_info < (3,),
                   'PCollVisualization is not supported on Python 2.')
  @patch('apache_beam.runners.interactive.display.pcoll_visualization'
         '.PCollVisualization._to_element_list', _mock_to_element_list)
  def test_dynamical_plotting_return_handle(self):
    h = pv.visualize(self._pcoll, dynamical_plotting_interval=1)
    self.assertIsInstance(h, timeloop.Timeloop)
    h.stop()

  @unittest.skipIf(sys.version_info < (3,),
                   'PCollVisualization is not supported on Python 2.')
  @patch('apache_beam.runners.interactive.display.pcoll_visualization'
         '.PCollVisualization._to_element_list', _mock_to_element_list)
  @patch('apache_beam.runners.interactive.display.pcoll_visualization'
         '.PCollVisualization.display_facets')
  def test_dynamical_plotting_update_same_display(self,
                                                  mocked_display_facets):
    # Starts async dynamical plotting.
    h = pv.visualize(self._pcoll, dynamical_plotting_interval=0.001)
    # Blocking so the above async task can execute a few iterations.
    time.sleep(0.1)
    # The first iteration doesn't provide updating_pv to display_facets.
    _, first_kwargs = mocked_display_facets.call_args_list[0]
    self.assertEqual(first_kwargs, {})
    # The following iterations use the same updating_pv to display_facets and so
    # on.
    _, second_kwargs = mocked_display_facets.call_args_list[1]
    updating_pv = second_kwargs['updating_pv']
    for call in mocked_display_facets.call_args_list[2:]:
      _, kwargs = call
      self.assertIs(kwargs['updating_pv'], updating_pv)
    h.stop()

  @unittest.skipIf(sys.version_info < (3,),
                   'PCollVisualization is not supported on Python 2.')
  @patch('apache_beam.runners.interactive.display.pcoll_visualization'
         '.PCollVisualization._to_element_list', _mock_to_element_list)
  @patch('timeloop.Timeloop.stop')
  def test_auto_stop_dynamical_plotting_when_job_is_terminated(self,
                                                               mocked_timeloop):
    fake_pipeline_result = runner.PipelineResult(runner.PipelineState.RUNNING)
    ie.current_env().set_pipeline_result(self._p, fake_pipeline_result)
    # Starts non-stopping async dynamical plotting until the job is terminated.
    pv.visualize(self._pcoll, dynamical_plotting_interval=0.001)
    # Blocking so the above async task can execute a few iterations.
    time.sleep(0.1)
    mocked_timeloop.assert_not_called()
    fake_pipeline_result = runner.PipelineResult(runner.PipelineState.DONE)
    ie.current_env().set_pipeline_result(self._p, fake_pipeline_result)
    # Blocking so the above async task can execute a few iterations.
    time.sleep(0.1)
    mocked_timeloop.assert_called()


if __name__ == '__main__':
  unittest.main()