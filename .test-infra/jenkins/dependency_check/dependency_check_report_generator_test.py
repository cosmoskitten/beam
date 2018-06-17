#!/usr/bin/env python
#
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#   This script performs testing of scenarios from verify_performance_test_results.py
#

import unittest, mock
from mock import patch
from datetime import datetime
from dependency_check_report_generator import prioritize_dependencies


_PROJECT_ID = 'mock-apache-beam-testing'
_DATASET_ID = 'mock-beam_dependency_states'
_TABLE_ID = 'mock-java_dependency_states'
_SDK_TYPE = 'JAVA'

# initialize current/latest version release dates for low-priority (lp) and high-priority (hp) dependencies
_LP_CURR_VERSION_DATE = datetime.strptime('2000-01-01', '%Y-%m-%d')
_LATEST_VERSION_DATE = datetime.strptime('2000-01-01', '%Y-%m-%d')
_HP_CURR_VERSION_DATE = datetime.strptime('1999-01-01', '%Y-%m-%d')

class DependencyCheckReportGeneratorTest(unittest.TestCase):
  """Tests for `dependency_check_report_generator.py`."""

  def setUp(self):
    print "Test name:", self._testMethodName


  @patch('google.cloud.bigquery.Client')
  @patch('bigquery_client_utils.BigQueryClientUtils')
  def test_empty_dep_input(self, *args):
    report = prioritize_dependencies([], _SDK_TYPE, _PROJECT_ID, _DATASET_ID, _TABLE_ID)
    self.assertEqual(len(report), 0)


  @patch('google.cloud.bigquery.Client')
  @patch('bigquery_client_utils.BigQueryClientUtils.query_dep_release_date', side_effect = [_LP_CURR_VERSION_DATE, _LATEST_VERSION_DATE,
                                                                                            _LP_CURR_VERSION_DATE, _LATEST_VERSION_DATE,
                                                                                            _HP_CURR_VERSION_DATE, _LATEST_VERSION_DATE,
                                                                                            _LP_CURR_VERSION_DATE, _LATEST_VERSION_DATE])
  def test_normal_dep_input(self, *args):
    deps = [
      " - group1:artifact1 [1.0.0 -> 3.0.0]",
      " - group2:artifact2 [1.0.0 -> 1.3.0]",
      " - group3:artifact3 [1.0.0 -> 1.1.0]",
      " - group4:artifact4 [1.0.0 -> 1.1.0]"
    ]
    report = prioritize_dependencies(deps, _SDK_TYPE, _PROJECT_ID, _DATASET_ID, _TABLE_ID)
    self.assertEqual(len(report), 3)
    self.assertIn('group1:artifact1', report[0])
    self.assertIn('group2:artifact2', report[1])
    self.assertIn('group3:artifact3', report[2])


if __name__ == '__main__':
  unittest.main()