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

"""Unit tests for the ValueProvider class."""


import unittest

from apache_beam.utils.pipeline_options import PipelineOptions
from apache_beam.utils.value_provider import RuntimeValueProvider


class ValueProviderTests(unittest.TestCase):
  def test_set_runtime_option(self):
    # define options, regular and of vp type
    class UserOptions(PipelineOptions):
      # pass
      @classmethod
      def _add_argparse_args(cls, parser):
        # pass
        parser.add_value_provider_argument(
            '--vp_arg',
            help='This flag is a value provider')

        parser.add_value_provider_argument(
            '--vp_arg2',
            default=1,
            type=int)

    # provide values at graph-construction time
    # (options not provided become of the type runtime vp)
    options = UserOptions([])
    assert options.vp_arg.is_accessible() is False
    assert options.vp_arg2.is_accessible() is False
    # provide values at job-execution time
    RuntimeValueProvider.set_runtime_options({'vp_arg': 'b'})
    assert options.vp_arg.is_accessible() is True
    assert options.vp_arg.get() == 'b'
    assert options.vp_arg2.is_accessible() is True
    assert options.vp_arg2.get() == 1
