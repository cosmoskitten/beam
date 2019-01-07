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

"""Unittest for GCP Bigtable testing."""
from __future__ import absolute_import

from apache_beam.options.pipeline_options import PipelineOptions


class WriteBigtableOptions(PipelineOptions):
  """ Create the Pipeline Options to set ReadBigtable/WriteBigtable.
  You can create and use this class in the Template, with a certainly steps.
  """
  @classmethod
  def _add_argparse_args(cls, parser):
    PipelineOptions._add_argparse_args(parser)
    parser.add_argument('--project', required=False)
    parser.add_argument('--instance', required=True)
    parser.add_argument('--table', required=True)
    parser.add_argument('--app_profie_id', required=False)
