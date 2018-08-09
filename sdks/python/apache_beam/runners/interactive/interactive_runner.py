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

"""A runner that allows running of Beam pipelines interactively.

This module is experimental. No backwards-compatibility guarantees.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import logging

import apache_beam as beam
from apache_beam import runners
from apache_beam.runners.direct import direct_runner
from apache_beam.runners.interactive import cache_manager as cache
from apache_beam.runners.interactive import display_manager
from apache_beam.runners.interactive import pipeline_analyzer

# size of PCollection samples cached.
SAMPLE_SIZE = 8


class InteractiveRunner(runners.PipelineRunner):
  """An interactive runner for Beam Python pipelines.

  Allows interactively building and running Beam Python pipelines.
  """

  def __init__(self, underlying_runner=None, cache_dir=None):
    # TODO(qinyeli, BEAM-4755) remove explicitly overriding underlying runner
    # once interactive_runner works with FnAPI mode
    self._underlying_runner = (underlying_runner
                               or direct_runner.BundleBasedDirectRunner())
    self._cache_manager = cache.FileBasedCacheManager(cache_dir)
    self._in_session = False

  def start_session(self):
    """Start the session that keeps back-end managers and workers alive.
    """
    if self._in_session:
      return

    enter = getattr(self._underlying_runner, '__enter__', None)
    if enter is not None:
      logging.info('Starting session.')
      self._in_session = True
      enter()
    else:
      logging.error('Keep alive not supported.')

  def end_session(self):
    """End the session that keeps backend managers and workers alive.
    """
    if not self._in_session:
      return

    exit = getattr(self._underlying_runner, '__exit__', None)
    if exit is not None:
      self._in_session = False
      logging.info('Ending session.')
      exit(None, None, None)

  def cleanup(self):
    self._cache_manager.cleanup()

  def apply(self, transform, pvalueish):
    # TODO(qinyeli, BEAM-646): Remove runner interception of apply.
    return self._underlying_runner.apply(transform, pvalueish)

  def run_pipeline(self, pipeline):
    if not hasattr(self, '_desired_cache_labels'):
      self._desired_cache_labels = set()

    # Invoke a round trip through the runner API. This makes sure the Pipeline
    # proto is stable.
    pipeline = beam.pipeline.Pipeline.from_runner_api(
        pipeline.to_runner_api(),
        pipeline.runner,
        pipeline._options)

    # Snapshot the pipeline in a portable proto before mutating it.
    pipeline_proto, original_context = pipeline.to_runner_api(
        return_context=True)
    pcolls_to_pcoll_id = self._pcolls_to_pcoll_id(pipeline, original_context)

    analyzer = pipeline_analyzer.PipelineAnalyzer(self._cache_manager,
                                                  pipeline_proto,
                                                  self._underlying_runner,
                                                  self._desired_cache_labels)
    pipeline_to_execute = beam.pipeline.Pipeline.from_runner_api(
        analyzer.pipeline_proto_to_execute(),
        self._underlying_runner,
        pipeline._options)

    pipeline_info = pipeline_analyzer.PipelineInfo(pipeline_proto.components)

    display = display_manager.DisplayManager(
        pipeline_info=pipeline_info,
        pipeline_proto=pipeline_proto,
        caches_used=analyzer.caches_used(),
        cache_manager=self._cache_manager,
        referenced_pcollections=analyzer.top_level_referenced_pcollection_ids(),
        required_transforms=analyzer.top_level_required_transforms())
    display.start_periodic_update()
    result = pipeline_to_execute.run()
    result.wait_until_finish()
    display.stop_periodic_update()

    return PipelineResult(result, self, pipeline_info, self._cache_manager,
                          pcolls_to_pcoll_id)

  def _pcolls_to_pcoll_id(self, pipeline, original_context):
    """Returns a dict mapping PCollections string to PCollection IDs.

    Using a PipelineVisitor to iterate over every node in the pipeline,
    records the mapping from PCollections to PCollections IDs. This mapping
    will be used to query cached PCollections.

    Args:
      pipeline: (pipeline.Pipeline)
      original_context: (pipeline_context.PipelineContext)

    Returns:
      (dict from str to str) a dict mapping str(pcoll) to pcoll_id.
    """
    pcolls_to_pcoll_id = {}

    from apache_beam.pipeline import PipelineVisitor  # pylint: disable=import-error

    class PCollVisitor(PipelineVisitor):  # pylint: disable=used-before-assignment
      """"A visitor that records input and output values to be replaced.

      Input and output values that should be updated are recorded in maps
      input_replacements and output_replacements respectively.

      We cannot update input and output values while visiting since that
      results in validation errors.
      """

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        for pcoll in transform_node.outputs.values():
          pcolls_to_pcoll_id[str(pcoll)] = original_context.pcollections.get_id(
              pcoll)

    pipeline.visit(PCollVisitor())
    return pcolls_to_pcoll_id


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


class PipelineResult(beam.runners.runner.PipelineResult):
  """Provides access to information about a pipeline."""

  def __init__(self, underlying_result, runner, pipeline_info, cache_manager,
               pcolls_to_pcoll_id):
    super(PipelineResult, self).__init__(underlying_result.state)
    self._runner = runner
    self._pipeline_info = pipeline_info
    self._cache_manager = cache_manager
    self._pcolls_to_pcoll_id = pcolls_to_pcoll_id

  def _cache_label(self, pcoll):
    pcoll_id = self._pcolls_to_pcoll_id[str(pcoll)]
    return self._pipeline_info.derivation(pcoll_id).cache_label()

  def wait_until_finish(self):
    # PipelineResult is not constructed until pipeline execution is finished.
    return

  def get(self, pcoll):
    cache_label = self._cache_label(pcoll)
    if self._cache_manager.exists('full', cache_label):
      pcoll_list, _ = self._cache_manager.read('full', cache_label)
      return pcoll_list
    else:
      self._runner._desired_cache_labels.add(cache_label)  # pylint: disable=protected-access
      raise ValueError('PCollection not available, please run the pipeline.')

  def sample(self, pcoll):
    cache_label = self._cache_label(pcoll)
    if self._cache_manager.exists('sample', cache_label):
      return self._cache_manager.read('sample', cache_label)
    else:
      self._runner._desired_cache_labels.add(cache_label)  # pylint: disable=protected-access
      raise ValueError('PCollection not available, please run the pipeline.')
