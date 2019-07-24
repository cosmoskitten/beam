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

from apache_beam.transforms.external import External


class GenerateSequence(External):
  """
    An external PTransform which provides a bounded or unbounded stream of
    integers.

    Note: To use this transform, you need to start the Java expansion service.
    Please refer to the portability documentation on how to do that. The
    expansion service address has to be provided when instantiating this
    transform. During pipeline translation this transform will be replaced by
    the Java SDK's GenerateSequence.

    If you start Flink's job server, the expansion service will be started on
    port 8097. This is also the configured default for this transform. For a
    different address, please set the expansion_service parameter.

    For more information see:
    - https://beam.apache.org/documentation/runners/flink/
    - https://beam.apache.org/roadmap/portability/

    Note: Runners need to support translating Read operations in order to use
    this source. At the moment only the Flink Runner supports this.

    Experimental; no backwards compatibility guarantees.
  """
  _urn = 'beam:external:java:generate_sequence:v1'

  def __init__(self, start, stop=None,
               elements_per_period=None, max_read_time=None,
               expansion_service=None):
    super(GenerateSequence, self).__init__(expansion_service)
    self.start = start
    self.stop = stop
    self.elements_per_period = elements_per_period
    self.max_read_time = max_read_time

  def get_config(self):
    return {
      'start': self.start,
      'stop': self.stop,
      'elements_per_period': self.elements_per_period,
      'max_read_time': self.max_read_time,
    }
