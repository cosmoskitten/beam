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
"""Unit tests for the apiclient module."""

import unittest

import hamcrest as hc

from apache_beam.internal import apiclient
from apache_beam.utils.options import PipelineOptions
from apache_beam.runners.dataflow_runner import DataflowPipelineRunner
from apache_beam.transforms.display import HasDisplayData
from apache_beam.transforms.display import DisplayDataItem


class UtilTest(unittest.TestCase):

  def test_create_application_client(self):
    pipeline_options = PipelineOptions()
    apiclient.DataflowApplicationClient(
        pipeline_options,
        DataflowPipelineRunner.BATCH_ENVIRONMENT_MAJOR_VERSION)

  def test_unsupported_type_display_data(self):
    it = DisplayDataItem({'key': 'value'})

    class MyDisplayComponent(HasDisplayData):
      def display_data(self):
        return {'item_key': self.it}

      def __init__(self, it):
        self.it = it

    with self.assertRaises(ValueError):
      apiclient.Environment.format_options_display_data(MyDisplayComponent(it))

  def test_create_list_display_data(self):
    flags = ['--extra_package', 'package1', '--extra_package', 'package2']
    pipeline_options = PipelineOptions(flags=flags)
    items = apiclient.Environment.format_options_display_data(pipeline_options)
    hc.assert_that(
        [{'type': 'STRING',
          'namespace': 'apache_beam.utils.options.PipelineOptions',
          'value': 'package1, package2', 'key': 'extra_packages'}],
        hc.contains_inanyorder(*items))


if __name__ == '__main__':
  unittest.main()
