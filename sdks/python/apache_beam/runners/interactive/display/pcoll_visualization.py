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

from enum import Enum
from flatten_dict import flatten
import apache_beam as beam
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import pipeline_instrument as instr


class VisualizationType(Enum):
  SCATTER = 1
  LINE = 2
  BAR = 3
  TABLE = 4


class PCollData(object):
  def __init__(self, pcoll):
    if not isinstance(pcoll, beam.pvalue.PCollection):
      raise ValueError('pcoll should be apache_beam.pvalue.PCollection')
    self._pcoll = pcoll
    self._pin = instr.PipelineInstrument(pcoll.pipeline)

  def to_element_list(self):
    key = self._pin.cache_key(self._pcoll)
    pcoll_list = []
    if ie.current_env().cache_manager().exists('full', key):
      pcoll_list, _ = ie.current_env().cache_manager().read('full', key)
    return pcoll_list, pcoll.element_type

  def to_flat_dict_list(self):
    pcoll_list, element_type = self.to_element_list()
    flatten_pcoll_list = []
    if issubclass(self._pcoll, object):
      flatten_pcoll_list = flatten(vars(pcoll_list), reducer=_key_reducer)
    if element_type in (list, tuple):
      flatten_pcoll_list = flatten(pcoll_list, reducer=_key_reducer)

    return flatten_pcoll_list

  def to_column_list(self):
    pass


def _key_reducer(k1, k2):
  if k1 is None:
    return str(k2)
  return str(k1) + '/' + str(k2)
