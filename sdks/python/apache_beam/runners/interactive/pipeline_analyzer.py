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

"""Analyzes and modifies the pipeline that utilize the PCollection cache.

This module is experimental. No backwards-compatibility guarantees.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import copy

import apache_beam as beam
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.interactive import cache_manager as cache


class PipelineAnalyzer(object):
  def __init__(self, cache_manager, pipeline_proto, underlying_runner,
               desired_cache_labels=None):
    self._cache_manager = cache_manager
    self._pipeline_proto = pipeline_proto
    self._underlying_runner = underlying_runner
    self._desired_cache_labels = desired_cache_labels or []

    self._pipeline_proto_to_execute = None
    self._top_level_referenced_pcollection_ids = None
    self._top_level_required_transforms = None

    self._analyze_pipeline()

  def _analyze_pipeline(self):
    pipeline, context = beam.pipeline.Pipeline.from_runner_api(
        self._pipeline_proto,
        self._underlying_runner,
        options=None,
        return_context=True)

    # context returned from to_runner_api is more informative than that returned
    # from from_runner_api.
    _, context = pipeline.to_runner_api(return_context=True)

    pipeline_info = PipelineInfo(self._pipeline_proto.components)
    self._caches_used = set()

    def _producing_transforms(pcoll_id, leaf=False):
      """Returns PTransforms (and their names) that produces the given PColl."""
      if pcoll_id in _producing_transforms.analyzed_pcoll_ids:
        return
      else:
        _producing_transforms.analyzed_pcoll_ids.add(pcoll_id)

      derivation = pipeline_info.derivation(pcoll_id)
      if self._cache_manager.exists('full', derivation.cache_label()):
        # If the PCollection is cached, yield ReadCache PTransform that reads
        # the PCollection and all its sub PTransforms.
        if not leaf:
          self._caches_used.add(pcoll_id)

          cache_label = pipeline_info.derivation(pcoll_id).cache_label()
          dummy_pcoll = pipeline | 'Load%s' % cache_label >> cache.ReadCache(
              self._cache_manager, cache_label)

          # Find the top level ReadCache composite PTransform.
          read_cache = self._top_level_producer(dummy_pcoll)
          for transform in self._include_subtransforms(read_cache):
            transform_id = context.transforms.get_id(transform)
            transform_proto = transform.to_runner_api(context)
            if dummy_pcoll in transform.outputs.values():
              transform_proto.outputs['None'] = pcoll_id
            yield transform_id, transform_proto

      else:
        pcoll = context.pcollections.get_by_id(pcoll_id)
        top_level_transform = self._top_level_producer(pcoll)
        for transform in self._include_subtransforms(top_level_transform):
          transform_id = context.transforms.get_id(transform)
          transform_proto = context.transforms.get_proto(transform)

          for input_id in transform_proto.inputs.values():
            for yielded in _producing_transforms(input_id):
              yield yielded
          yield transform_id, transform_proto

    desired_pcollections = self._desired_pcollections(pipeline_info)

    required_transforms = collections.OrderedDict()
    _producing_transforms.analyzed_pcoll_ids = set()
    for pcoll_id in desired_pcollections:
      # TODO(qinyeli): Collections consumed by no-output transforms.
      required_transforms.update(_producing_transforms(pcoll_id, True))

    top_level_required_transforms = self._filter_top_level_transforms(
        required_transforms)
    top_level_referenced_pcollection_ids = self._referenced_pcollection_ids(
        top_level_required_transforms)

    pdones = []
    for pcoll_id in pipeline_info.all_pcollections():
      if pcoll_id not in top_level_referenced_pcollection_ids:
        continue

      cache_label = pipeline_info.derivation(pcoll_id).cache_label()
      pcoll = context.pcollections.get_by_id(pcoll_id)

      if (pcoll_id in desired_pcollections
          and not pcoll_id in self._caches_used):
        pdone = pcoll | 'CacheFull%s' % cache_label >> cache.WriteCache(
            self._cache_manager, cache_label)
        pdones.append(pdone)

      if (pcoll_id in top_level_referenced_pcollection_ids
          and not self._cache_manager.exists('sample', cache_label)):
        pdone = pcoll | 'CacheSample%s' % cache_label >> cache.WriteCache(
            self._cache_manager, cache_label, sample=True,
            sample_size=10)
        pdones.append(pdone)

    for pdone in pdones:
      write_cache = self._top_level_producer(pdone)
      for transform in self._include_subtransforms(write_cache):
        transform_id = context.transforms.get_id(transform)
        transform_proto = transform.to_runner_api(context)
        required_transforms[transform_id] = transform_proto

    top_level_required_transforms = self._filter_top_level_transforms(
        required_transforms)

    required_transforms['_root'] = beam_runner_api_pb2.PTransform(
        subtransforms=top_level_required_transforms.keys())

    referenced_pcollection_ids = self._referenced_pcollection_ids(
        required_transforms)
    referenced_pcollections = {}
    for pcoll_id in referenced_pcollection_ids:
      obj = context.pcollections.get_by_id(pcoll_id)
      referenced_pcollections[pcoll_id] = context.pcollections.get_proto(obj)

    pipeline_to_execute = copy.deepcopy(self._pipeline_proto)
    pipeline_to_execute.root_transform_ids[:] = ['_root']
    set_proto_map(pipeline_to_execute.components.transforms,
                  required_transforms)
    set_proto_map(pipeline_to_execute.components.pcollections,
                  referenced_pcollections)
    set_proto_map(pipeline_to_execute.components.coders,
                  context.to_runner_api().coders)
    set_proto_map(pipeline_to_execute.components.windowing_strategies,
                  context.to_runner_api().windowing_strategies)

    self._pipeline_proto_to_execute = pipeline_to_execute
    self._top_level_referenced_pcollection_ids = top_level_referenced_pcollection_ids # pylint: disable=line-too-long
    self._top_level_required_transforms = top_level_required_transforms

  # Getters

  def pipeline_proto_to_execute(self):
    """Returns Pipeline proto to be executed.
    """
    return self._pipeline_proto_to_execute

  def top_level_referenced_pcollection_ids(self):
    """Returns an array of top level referenced PCollection IDs.
    """
    return self._top_level_referenced_pcollection_ids

  def top_level_required_transforms(self):
    """Returns a dict mapping ID to proto of top level PTransforms.
    """
    return self._top_level_required_transforms

  def caches_used(self):
    """Returns an array of PCollection IDs to read from cache.
    """
    return self._caches_used

  # Helper methods for _ananlyze_pipeline()

  def _desired_pcollections(self, pipeline_info):
    """Returns IDs of desired PCollections.

    Args:
      pipeline_info: (PipelineInfo) info of the original pipeline.

    Returns:
      (Set[str]) a set of PCollections IDs of either leaf PCollections or
      PCollections referenced by the user. These PCollections should be cached
      at the end of pipeline execution.
    """
    desired_pcollections = set(pipeline_info.leaf_pcollections())
    for pcoll_id in pipeline_info.all_pcollections():
      cache_label = pipeline_info.derivation(pcoll_id).cache_label()

      if cache_label in self._desired_cache_labels:
        desired_pcollections.add(pcoll_id)
    return desired_pcollections

  def _referenced_pcollection_ids(self, required_transforms):
    """Returns PCollection IDs referenced in the given transforms.

    Args:
      transforms: (Dict[str, PTransform proto]) mapping ID to protos.

    Returns:
      (Set[str]) PCollection IDs referenced in the given transforms.
    """
    referenced_pcollection_ids = set()
    for transform_proto in required_transforms.values():
      for pcoll_id in transform_proto.inputs.values():
        referenced_pcollection_ids.add(pcoll_id)
      for pcoll_id in transform_proto.outputs.values():
        referenced_pcollection_ids.add(pcoll_id)
    return referenced_pcollection_ids

  def _top_level_producer(self, pcoll):
    """Given a PCollection, returns the top level producing PTransform.

    Args:
      pcoll: (PCollection)

    Returns:
      (PTransform) top level producing PTransform of pcoll.
    """
    top_level_transform = pcoll.producer
    while top_level_transform.parent.parent:
      top_level_transform = top_level_transform.parent
    return top_level_transform

  def _include_subtransforms(self, transform):
    """Depth-first yield the PTransform itself and its sub transforms.

    Args:
      transform: (PTransform)

    Yields:
      The input PTransform itself and all its sub transforms.
    """
    yield transform
    for subtransform in transform.parts[::-1]:
      for yielded in self._include_subtransforms(subtransform):
        yield yielded

  def _filter_top_level_transforms(self, transforms):
    """Given a dict of PTransforms, filters only the top level PTransform.

    Args:
      transforms: (Dict[str, PTransform proto]) mapping ID to proto.

    Returns:
      (Dict[str, PTransform proto]) mapping top level PTransform ID to proto.

    """
    top_level_transforms = collections.OrderedDict()
    for transform_id, transform_proto in transforms.items():
      if '/' not in transform_id:
        top_level_transforms[transform_id] = transform_proto
    return top_level_transforms


class PipelineInfo(object):
  """Provides access to pipeline metadata."""

  def __init__(self, proto):
    self._proto = proto
    self._producers = {}
    self._consumers = collections.defaultdict(list)
    for transform_id, transform_proto in self._proto.transforms.items():
      if transform_proto.subtransforms:
        continue
      for tag, pcoll_id in transform_proto.outputs.items():
        self._producers[pcoll_id] = transform_id, tag
      for pcoll_id in transform_proto.inputs.values():
        self._consumers[pcoll_id].append(transform_id)
    self._derivations = {}

  def all_pcollections(self):
    return self._proto.pcollections.keys()

  def leaf_pcollections(self):
    for pcoll_id in self._proto.pcollections:
      if not self._consumers[pcoll_id]:
        yield pcoll_id

  def producer(self, pcoll_id):
    return self._producers[pcoll_id]

  def derivation(self, pcoll_id):
    """Returns the Derivation corresponding to the PCollection."""
    if pcoll_id not in self._derivations:
      transform_id, output_tag = self._producers[pcoll_id]
      transform_proto = self._proto.transforms[transform_id]
      self._derivations[pcoll_id] = Derivation({
          input_tag: self.derivation(input_id)
          for input_tag, input_id in transform_proto.inputs.items()
      }, transform_proto, output_tag)
    return self._derivations[pcoll_id]


class Derivation(object):
  """Records derivation info of a PCollection. Helper for PipelineInfo."""

  def __init__(self, inputs, transform_proto, output_tag):
    """Constructor of Derivation.

    Args:
      inputs: (Dict[str, str]) a dict that contains input PCollections to the
        producing PTransform of the output PCollection. Maps local names to IDs.
      transform_proto: (Transform proto) the producing PTransform of the output
        PCollection.
      output_tag: (str) local name of the output PCollection; this is the
        PCollection in analysis.
    """
    self._inputs = inputs
    self._transform_info = {
        # TODO(qinyeli): remove name field when collision is resolved.
        'name': transform_proto.unique_name,
        'urn': transform_proto.spec.urn,
        'payload': transform_proto.spec.payload.decode('latin1')
    }
    self._output_tag = output_tag
    self._hash = None

  def __eq__(self, other):
    if isinstance(other, Derivation):
      # pylint: disable=protected-access
      return (self._inputs == other._inputs and
              self._transform_info == other._transform_info)

  def __hash__(self):
    if self._hash is None:
      self._hash = (hash(tuple(sorted(self._transform_info.items())))
                    + sum(hash(tag) * hash(input)
                          for tag, input in self._inputs.items())
                    + hash(self._output_tag))
    return self._hash

  def cache_label(self):
    # TODO(qinyeli): Collision resistance?
    return 'Pcoll-%x' % abs(hash(self))

  def json(self):
    return {
        'inputs': self._inputs,
        'transform': self._transform_info,
        'output_tag': self._output_tag
    }

  def __repr__(self):
    return str(self.json())


# TODO(qinyeli) move to proto_utils
def set_proto_map(proto_map, new_value):
  proto_map.clear()
  for key, value in new_value.items():
    proto_map[key].CopyFrom(value)
