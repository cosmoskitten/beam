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

"""
A PTransform that provides a bounded or unbounded stream of integers.
"""
from __future__ import absolute_import

from apache_beam import PTransform, ExternalTransform
from apache_beam import pvalue
from apache_beam.portability.api.external_transforms_pb2 import GenerateSequencePayload
from apache_beam.portability.common_urns import generate_sequence


class GenerateSequence(PTransform):

  def __init__(self, start, stop, elements_per_period, max_read_time, expansion_service, **kwargs):
    super(GenerateSequence, self).__init__(kwargs)
    self.start = start
    self.stop = stop
    self.elements_per_period = elements_per_period
    self.max_read_time = max_read_time
    # TODO This should really come from pipeline options
    self.expansion_service = expansion_service

  def expand(self, pbegin):
    assert isinstance(pbegin, pvalue.PBegin), (
        'Input to transform must be a PBegin but found %s' % pbegin)
    payload = GenerateSequencePayload(
      start=self.start,
      stop=self.stop,
      elements_per_period=self.elements_per_period,
      max_read_time=self.max_read_time
    )
    return ExternalTransform(generate_sequence.URN, payload, self.expansion_service)