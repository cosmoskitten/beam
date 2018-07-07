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

"""For generating Beam pipeline graph in DOT representation.

This module is experimental. No backwards-compatibility guarantees.
"""

class PipelineGraph(object):
  """Creates a DOT representation of the pipeline. Thread-safe."""

  def __init__(self, pipeline_proto, pcollection_attrs=None, transform_attrs=None):

    # A dict from PCollection ID to a list of its consuming Transform IDs
    self._consumers = collections.defaultdict(list)
    # A dict from PCollection ID to its producing Transform ID
    self._producers = {}

    self._lock = threading.Lock()
    self._graph = None

    self._generate_pipeline_graph(pipeline_proto)

  def _generate_pipeline_graph(self, pipeline_proto):
    transforms = self._pipeline_proto.components.transforms

    def is_top_level_transform(transform):
      return transform.unique_name and '/' not in transform.unique_name

    for transform_id, transform in transforms.items():
      if not is_top_level_transform(transform):
        continue
      for pcoll_id in transform.inputs.values():
        self._consumers[pcoll_id].append(transform_id)
      for pcoll_id in transform.outputs.values():
        self._producers[pcoll_id] = transform_id

    # A dict from vertex name (i.e. PCollection ID) to its attributes.
    vertex_set = collections.defaultdict(dict)
    # A dict from vertex name pairs defining the edge (i.e. a pair of PTransform
    # IDs defining the PCollection) to its attributes.
    edge_set = collections.defaultdict(dict)

    for transform_id, transform in transforms.items():
      if not is_top_level_transform(transform):
        continue
      vertex_set[transform_id] = {}
      
      for pcoll_id in transform.outputs.values():

        # For PCollections without consuming PTransforms, we add an invisible
        # PTransform node as the consumer.
        if pcoll_id not in self._consumers:
          invisible_leaf_id = 'leaf%s' % (hash(pcollection_id) % 10000)
          pcoll_consumers = [invisible_leaf_id]
          vertex_dict[invisible_leaf_id] = {'style': 'invis'}

