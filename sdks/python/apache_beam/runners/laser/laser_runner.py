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

"""DirectRunner, executing on the local machine.

The DirectRunner is a runner implementation that executes the entire
graph of transformations belonging to a pipeline on the local machine.
"""

# from __future__ import absolute_import

import cPickle as pickle
import json
import logging
import multiprocessing
from Queue import Queue
import random
import socket
import struct
import subprocess
import sys
import threading
import time

# import collections
# import logging

# from google.protobuf import wrappers_pb2

import apache_beam as beam
from apache_beam.internal import pickler
# from apache_beam import typehints
# from apache_beam.metrics.execution import MetricsEnvironment
# from apache_beam.options.pipeline_options import DirectOptions
# from apache_beam.options.pipeline_options import StandardOptions
# from apache_beam.options.value_provider import RuntimeValueProvider
# from apache_beam.pvalue import PCollection
# from apache_beam.runners.direct.bundle_factory import BundleFactory
# from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineRunner
# from apache_beam.runners.runner import PipelineState
# from apache_beam.runners.runner import PValueCache
# from apache_beam.transforms.core import _GroupAlsoByWindow
# from apache_beam.transforms.core import _GroupByKeyOnly
# from apache_beam.transforms.ptransform import PTransform

# __all__ = ['LaserRunner']

from apache_beam.runners.dataflow.internal.names import PropertyNames

from apache_beam.runners.laser.channels import ChannelConfig
from apache_beam.runners.laser.channels import LinkMode
from apache_beam.runners.laser.channels import LinkStrategy
from apache_beam.runners.laser.channels import LinkStrategyType
from apache_beam.runners.laser.channels import Interface
from apache_beam.runners.laser.channels import set_channel_config
from apache_beam.runners.laser.channels import get_channel_manager
from apache_beam.runners.laser.channels import remote_method
from apache_beam.runners.dataflow import DataflowRunner
from apache_beam.runners.dataflow.native_io.iobase import NativeSource
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import operations
from apache_beam.io import iobase
from apache_beam.io import ReadFromText
from apache_beam import pvalue
from apache_beam.pvalue import PBegin
from apache_beam.runners.worker import operation_specs
# from apache_beam.runners.worker import operations
from apache_beam.utils.counters import CounterFactory
try:
  from apache_beam.runners.worker import statesampler
except ImportError:
  from apache_beam.runners.worker import statesampler_fake as statesampler


from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP

class StepGraph(object):
  def __init__(self):
    self.steps = []  # set?

    # The below are used for conevenience in construction of the StepGraph, but
    # not by any subsequent logic.
    self.transform_node_to_step = {}
    self.pcollection_to_node = {}

  # def register_origin_transform_node()
  def add_step(self, transform_node, step):
    self.steps.append(step)
    self.transform_node_to_step[transform_node] = step
    print 'TRANSFORM_NODE', transform_node
    print 'inputs', transform_node.inputs
    inputs = []
    for input_pcollection in transform_node.inputs:
      if isinstance(input_pcollection, PBegin):
        continue
      assert input_pcollection in self.pcollection_to_node
      pcollection_node = self.pcollection_to_node[input_pcollection]
      pcollection_node.add_consumer(step)
      inputs.append(pcollection_node)
    step.inputs = inputs
    print 'YO inputs', inputs

    # TODO: side inputs in this graph.

    outputs = {}
    for tag, output_pcollection in transform_node.outputs.iteritems():
      print 'TAG', tag, output_pcollection
      # TODO: do we want to associate system names here or somewhere?  might be useful for association with monitoring and such.
      pcollection_node = PCollectionNode(step, tag)
      self.pcollection_to_node[output_pcollection] = pcollection_node
      outputs[tag] = pcollection_node
    step.outputs = outputs
    print 'YO outputs', outputs
    # if original_node:  # original_transform_node?
    #   self.transform_node_to_step[original_node] = step
    # if input_step:  # TODO: should this be main_input?
    #   step._add_input(input_step)
    #   input_step._add_output(step)

  def get_step_from_node(self, transform_node):
    return self.transform_node_to_step[transform_node]

  def __repr__(self):
    return 'StepGraph(steps=%s)' % self.steps


class PCollectionNode(object):
  def __init__(self, step, tag):
    super(PCollectionNode, self).__init__()
    self.step = step
    self.tag = tag
    self.consumers = []

  def add_consumer(self, consumer_step):
    self.consumers.append(consumer_step)

  def __repr__(self):
    return 'PCollectionNode[%s.%s]' % (self.step.name, self.tag)


class WatermarkNode(object):
  def __init__(self):
    super(WatermarkNode, self).__init__()
    self.input_watermark = MIN_TIMESTAMP
    self.watermark_hold = MAX_TIMESTAMP
    self.output_watermark = MIN_TIMESTAMP
    self.watermark_parents = []
    self.watermark_children = []

  def add_dependent(self, dependent):
    self.watermark_children.append(dependent)
    dependent.watermark_parents.append(self)

  def _refresh_input_watermark(self):
    print '_refresh_input_watermark OLD', self, self.input_watermark
    new_input_watermark = MAX_TIMESTAMP
    if self.watermark_parents:
      new_input_watermark = min(parent.output_watermark for parent in self.watermark_parents)
    print '_refresh_input_watermark NEW', self, new_input_watermark
    if new_input_watermark > self.input_watermark:
      self._advance_input_watermark(new_input_watermark)

  def _refresh_output_watermark(self):
    new_output_watermark = min(self.input_watermark, self.watermark_hold)
    if new_output_watermark > self.output_watermark:
      print 'OUTPUT watermark advanced', self, new_output_watermark
      self.output_watermark = new_output_watermark
      for dependent in self.watermark_children:
        dependent._refresh_input_watermark()
    else:
      print 'OUTPUT watermark unchanged', self

  def set_watermark_hold(self, hold_time=None):
    # TODO: do we need some synchronization?
    if hold_time is None:
      hold_time = MAX_TIMESTAMP
    self.watermark_hold = hold_time
    self._refresh_output_watermark()

  def _advance_input_watermark(self, new_watermark):
    if new_watermark <= self.input_watermark:
      print 'not advancing input watermark', self
      return
    self.input_watermark = new_watermark
    print 'advancing input watermark', self
    self.input_watermark_advanced(new_watermark)
    self._refresh_output_watermark()



  def input_watermark_advanced(self, new_watermark):
    pass



# class StepInfo(object):
#   def __init__(self, input_pcollection, output_pcollections):
#     self.input_pcollection = input_pcollection
#     self.output_pcollections = output_pcollections

class Step(WatermarkNode):
  def __init__(self, name):
    super(Step, self).__init__()
    self.name = name
    self.inputs = []  # Should have one element except in case of Combine
    # self.side_input_steps
    self.outputs = {}

  # def _add_input(self, input_step):
  #   assert isinstance(input_step, Step)
  #   self.inputs.append(input_step)

  # def _add_output(self, output_step):  # add_consumer? what happens with named outputs? do we care?
  #   assert isinstance(output_step, Step)
  #   self.outputs.append(output_step)
  
  def copy(self):
    """Return copy of this Step, without its attached inputs or outputs."""
    raise NotImplementedError()

  def __repr__(self):
    return 'Step(%s, coder: %s)' % (self.name, getattr(self, 'element_coder', None))

class ReadStep(Step):
  def __init__(self, name, original_source_bundle, element_coder):
    super(ReadStep, self).__init__(name)
    self.original_source_bundle = original_source_bundle
    self.element_coder = element_coder

  def copy(self):
    return ReadStep(self.name, self.original_source_bundle, self.element_coder)

  def as_operation(self, unused_step_index):
    return operation_specs.WorkerRead(self.original_source_bundle, output_coders=[self.element_coder])


class ParDoFnData(object):
  def __init__(self, fn, args, kwargs, si_tags_and_types, windowing):
    self.fn = fn
    self.args = args
    self.kwargs = kwargs
    self.si_tags_and_types = si_tags_and_types
    self.windowing = windowing

  def __repr__(self):
    return 'ParDoFnData(fn: %s, args: %s, kwargs: %s, si_tags_and_types: %s, windowing: %s)' % (
        self.fn, self.args, self.kwargs, self.si_tags_and_types, self.windowing
      )

  def as_serialized_fn(self):
    return pickler.dumps((self.fn, self.args, self.kwargs, self.si_tags_and_types, self.windowing))


class ParDoStep(Step):
  def __init__(self, name, pardo_fn_data, element_coder, output_tags):
    super(ParDoStep, self).__init__(name)
    self.pardo_fn_data = pardo_fn_data
    self.element_coder = element_coder
    self.output_tags = output_tags

  def copy(self):
    return ParDoStep(self.name, self.pardo_fn_data, self.element_coder, self.output_tags)

  def as_operation(self, step_index):
    assert len(self.inputs) == 1
    return operation_specs.WorkerDoFn(
        serialized_fn=self.pardo_fn_data.as_serialized_fn(),
        output_tags=[PropertyNames.OUT] + ['%s_%s' % (PropertyNames.OUT, tag)
                                           for tag in self.output_tags],
        output_coders=[self.element_coder] * (len(self.output_tags) + 1),
        input=(step_index[self.inputs[0].step], 0),  # TODO: multiple output support.
        side_inputs=[])

class GroupByKeyOnlyStep(Step):
  def __init__(self, name, element_coder):
    super(GroupByKeyOnlyStep, self).__init__(name)
    self.element_coder = element_coder

  def copy(self):
    return GroupByKeyOnlyStep(self.element_coder)

class CompositeWatermarkNode(WatermarkNode):
  class InputWatermarkNode(WatermarkNode):
    def __init__(self, composite_node):
      super(CompositeWatermarkNode.InputWatermarkNode, self).__init__()
      self.composite_node = composite_node

    def input_watermark_advanced(self, new_watermark):
      self.composite_node.input_watermark_advanced(new_watermark)

  class OutputWatermarkNode(WatermarkNode):
    def __init__(self, composite_node):
      super(CompositeWatermarkNode.OutputWatermarkNode, self).__init__()

  def __init__(self):
    super(CompositeWatermarkNode, self).__init__()
    self.input_watermark_node = CompositeWatermarkNode.InputWatermarkNode(self)
    self.output_watermark_node = CompositeWatermarkNode.OutputWatermarkNode(self)

  # def set_watermark_hold(self, hold_time=None):  # TODO: should we remove this so it is more explicit?
  #   self.input_watermark_node.set_watermark_hold(hold_time=hold_time)


class WatermarkManager(object):
  def __init__(self):
    self.tracked_nodes = set()
    self.root_nodes = set()

  def track_nodes(self, watermark_node):
    start_node = watermark_node
    if isinstance(watermark_node, CompositeWatermarkNode):
      print 'track_nodes GOT COMPOSITE NODE', watermark_node
      start_node = watermark_node.input_watermark_node
    if start_node in self.tracked_nodes:
      return
    self.tracked_nodes.add(start_node)
    self.root_nodes.add(start_node)
    queue = Queue()
    queue.put(start_node)
    while not queue.empty():
      current = queue.get()
      print 'TRACK NODE', current
      for dependent in current.watermark_children:
        if dependent in self.tracked_nodes:
          self.root_nodes.discard(dependent)
        else:
          self.tracked_nodes.add(dependent)
          queue.put(dependent)

  def start(self):
    for node in self.root_nodes:
      node._refresh_input_watermark()






class ExecutionGraph(object):
  def __init__(self):
    self.stages = []

  def add_stage(self, stage):
    self.stages.append(stage)


class Stage(object):  # TODO: rename to ExecutionStage
  def __init__(self):
    super(Stage, self).__init__()



class SimpleShuffleDataset(object):
  def __init__(self):
    self.finalized = False
    self.items = []
    self.sorted_items = None
    self.uncommitted_items = {}
    self.committed_txns = set()
    self.lock = threading.Lock()

  def put(self, txn_id, key, value):
    assert isinstance(key, str)
    assert isinstance(value, str)
    with self.lock:
      if self.finalized:
        raise Exception('Dataset already finalized.')
      if txn_id in committed_txns:
        raise Exception('Can\'t write to already committed transaction: %s.' % txn_id)
      if txn_id not in self.uncommitted_items:
        self.uncommitted_items[txn_id] = []
      self.uncommitted_items[txn_id].append((key, value))

  def put_kvs(self, txn_id, kvs):
    for key, value in kvs:
      self.put(txn_id, key, value)


  def commit_transaction(self, txn_id):
    with self.lock:
      if self.finalized:
        raise Exception('Dataset already finalized.')
      if txn_id in committed_txns:
        raise Exception('Transaction already committed: %s.' % txn_id)
      if txn_id in self.uncommitted_items:
        self.items += self.uncommitted_items[txn_id]
        del self.uncommitted_items[txn_id]
      self.committed_txns.add(txn_id)


  def finalize(self):
    with self.lock:
      self.finalized = True
      self.sorted_items = sorted(self.items, key=lambda k, v: k)
      self.cumulative_size = []
      cumulative = 0
      for k, v in self.sorted_items:
        cumulative += len(k) + len(v)
        self.cumulative_size.append(cumulative)

  def _seek(self, lexicographic_position):
    # returns first index >= the lexicographic position.
    # if greater than all values, returns size of items list
    # else, if less-eq than all values, returns 0
    if lexicographic_position == LexicographicPosition.KEYSPACE_BEGINNING:
      return 0
    if lexicographic_position == LexicographicPosition.KEYSPACE_END:
      return len(self.items)

    # Do binary search on items for position.
    # Invariant: lower is always <= lex position, upper is always > lex position.
    lower = -1
    upper = len(self.items)
    while lower + 1 < upper:
      middle = (lower + upper) / 2
      if lexicographic_cmp(self.items[middle][0], lexicographic_position) >= 0:
        # key at middle is >= lex position
        lower, upper = lower, middle
      else:
        # key at middle is < lex position
        lower, upper = middle, upper

    first_index = upper

    # sanity check
    if len(self.items) == 0:
      assert first_index == 0  # TODO: what to do about this special case?
    if first_index == 0:
      assert lexicographic_cmp(self.items[0][0], lexicographic_position) >= 0
    elif first_index == len(self.items):
      assert lexicographic_cmp(self.items[-1][0], lexicographic_position) < 0
    else:
      assert lexicographic_cmp(self.items[first_index][0], lexicographic_position) >= 0
      assert lexicographic_cmp(self.items[first_index - 1][0], lexicographic_position) < 0

    return first_index


  def summarize_key_range(self, lexicographic_range):
    first_index = self._seek(lexicographic_range.start)
    one_past_last_index = self._seek(lexicographic_range.end)
    return KeyRangeSummary(one_past_last_index - first_index, self.cumulative_size[one_past_last_index])


class KeyRangeSummary(object):
  def __init__(self, item_count, byte_count):
    self.item_count = item_count
    self.byte_count = byte_count

class ShuffleWorkerInterface(Interface):
  @remote_method(int, int, list)
  def write(self, dataset_id, txn_id, kvs):
    raise NotImplementedError()

  @remote_method(int, int)
  def commit_transaction(self, dataset_id, txn_id):
    raise NotImplementedError()

  @remote_method(int)
  def finalize_dataset(self, dataset_id):
    raise NotImplementedError()


class LexicographicPosition(object):
  KEYSPACE_BEGINNING = 1
  KEYSPACE_END = 2


def lexicographic_cmp(left, right):
  if left == LexicographicPosition.KEYSPACE_BEGINNING:
    return -1
  if right == LexicographicPosition.KEYSPACE_END:
    return 1
  return cmp(left, right)


class LexicographicRange(object):
  def __init__(self, start, end):
    assert isinstance(start, str) or start == KEYSPACE_BEGINNING
    assert isinstance(end, str) or end == KEYSPACE_END
    self.start = start
    self.end = end



class SimpleShuffleWorker(ShuffleWorkerInterface):
  def __init__(self):
    self.datasets = {}

  # REMOTE METHOD
  def write(self, dataset_id, txn_id, kvs):
    self.datasets[dataset_id].put_kvs(txn_id, kvs)

  # REMOTE METHOD
  def commit_transaction(self, dataset_id, txn_id):
    self.datasets[dataset_id].commit_transaction(txn_id)

  def summarize_key_range(self, dataset_id, lexicographic_range):
    return self.datasets[dataset_id].summarize_key_range(lexicographic_range)


  # REMOTE METHOD
  def finalize_dataset(self, dataset_id):
    self.datasets[dataset_id].finalize()



class ShuffleFinalizeStage(Stage):
  def __init__(self, name, execution_context, dataset_id):
    super(ShuffleFinalizeStage, self).__init__()
    self.name = name
    self.execution_context = execution_context
    self.dataset_id = dataset_id

  def start(self):
    self.execution_context.shuffle_interface.finalize_dataset(dataset_id)


  def input_watermark_advanced(self, new_watermark):
    print 'ShuffleFinalizeStage input watermark ADVANCED', new_watermark
    if new_watermark == MAX_TIMESTAMP:
      self.start()  # TODO: reconcile all the run / start method names.
  





class FusedStage(Stage, CompositeWatermarkNode):

  def __init__(self, name, execution_context):
    super(FusedStage, self).__init__()
    self.name = name
    self.execution_context = execution_context

    self.step_to_original_step = {}
    self.original_step_to_step = {}
    self.read_step = None

    # Topologically sorted steps.
    self.steps = []
    # Step to index in self.steps.
    self.step_index = {}

    self.pending_work_item_ids = set()
    # Hold watermark on all steps until execution progress is made.
    self.input_watermark_node.set_watermark_hold(MIN_TIMESTAMP)

  def add_step(self, original_step):
    # when a FusedStage adds a step, the step is copied.
    step = original_step.copy()
    # self.step_to_original_step[step] = original_step
    self.original_step_to_step[original_step] = step
    # Replicate outputs.
    for tag, original_output_pcoll in original_step.outputs.iteritems():
      step.outputs[tag] = PCollectionNode(step, tag)

    if isinstance(step, ReadStep):
      assert not original_step.inputs
      assert not self.read_step
      self.read_step = step
      self.input_watermark_node.add_dependent(step)
    else:
      # Copy inputs.
      for original_input_pcoll in original_step.inputs:
        input_step = self.original_step_to_step[original_input_pcoll.step]
        input_pcoll = input_step.outputs[tag]
        step.inputs.append(input_pcoll)
        input_pcoll.add_consumer(step)
        input_step.add_dependent(step)
    self.steps.append(step)
    self.step_index[step] = len(self.steps) - 1

  def finalize_steps(self):
    for step in self.steps:
      # TODO: we actually only need to add the ones that don't have children
      step.add_dependent(self.output_watermark_node)


  def __repr__(self):
    return 'FusedStage(name: %s, steps: %s)' % (self.name, self.steps)

  def input_watermark_advanced(self, new_watermark):
    print 'FUSEDSTAGE input watermark ADVANCED', new_watermark
    if new_watermark == MAX_TIMESTAMP:
      self.start()  # TODO: reconcile all the run / start method names.

  def start(self):
    print 'STARTING FUSED STAGE', self

    print 'MAPTASK GENERATION!!'
    all_operations = list(step.as_operation(self.step_index) for step in self.steps)

    # Perform initial source splitting.
    read_op = all_operations[0]
    assert isinstance(read_op, operation_specs.WorkerRead)
    split_source_bundles = list(read_op.source.source.split(1024))
    print '!!! SPLIT OFF', split_source_bundles
    split_read_ops = []
    for source_bundle in split_source_bundles:
      new_read_op = operation_specs.WorkerRead(source_bundle, read_op.output_coders)
      split_read_ops.append(new_read_op)

    for removeme_i, split_read_op in enumerate(split_read_ops):
      ops = [split_read_op] + all_operations[1:]
      step_names = list('s%s' % ix for ix in range(len(self.steps)))
      system_names = step_names
      original_names = list(step.name for step in self.steps)
      map_task = operation_specs.MapTask(ops, self.name, system_names, step_names, original_names)
      print 'MAPTASK', map_task
      # counter_factory = CounterFactory()
      # state_sampler = statesampler.StateSampler(self.name, counter_factory)
      # map_executor = operations.SimpleMapTaskExecutor(map_task, counter_factory, state_sampler)
      # map_executor.execute()
      print 'SCHEDULING WORK (%d / %d)...' % (removeme_i + 1, len(split_read_ops))
      work_item_id = self.execution_context.work_manager.schedule_map_task(self, map_task)
      self.pending_work_item_ids.add(work_item_id)
    print 'DONE SCHEDULING WORK!'
    # self.input_watermark_node.set_watermark_hold(None)

  def report_work_completion(self, work_item_id):
    self.pending_work_item_ids.remove(work_item_id)
    print 'COMPLETION', self, work_item_id, self.pending_work_item_ids
    if not self.pending_work_item_ids:
      # Stage completed!  Let's release the watermark.
      self.input_watermark_node.set_watermark_hold(None)

  def initialize(self):
    print 'INIT'


class Executor(object):
  def __init__(self, execution_graph, execution_context):
    self.execution_graph = execution_graph
    self.execution_context = execution_context

  def run(self):
    print 'EXECUTOR RUN'
    print 'execution graph', self.execution_graph
    self.execution_context.work_manager.start()
    for fused_stage in self.execution_graph.stages:
      fused_stage.initialize()

    self.execution_context.watermark_manager.start()



class ExecutionContext(object):
  def __init__(self, work_manager, watermark_manager):
    self.work_manager = work_manager
    self.watermark_manager = watermark_manager


def generate_execution_graph(step_graph, execution_context):
  # TODO: if we ever support interactive pipelines or incremental execution,
  # we want to implement idempotent application of a step graph into updating
  # the execution graph.
  execution_graph = ExecutionGraph()
  root_steps = list(step for step in step_graph.steps if not step.inputs)
  to_process = Queue()
  for step in root_steps:
    to_process.put(step)
  seen = set(root_steps)

  # Grow fused stages through a breadth-first traversal.
  steps_to_fused_stages = {}
  while not to_process.empty():
    original_step = to_process.get()
    if isinstance(original_step, ReadStep):
      assert not original_step.inputs
      stage_name = 'S%02d' % len(steps_to_fused_stages)
      fused_stage = FusedStage(stage_name, execution_context)
      fused_stage.add_step(original_step)
      print 'fused_stage', original_step, fused_stage
      steps_to_fused_stages[original_step] = fused_stage
    elif isinstance(original_step, ParDoStep):
      assert len(original_step.inputs) == 1
      input_step = original_step.inputs[0].step
      print 'input_step', input_step, type(input_step)
      fused_stage = steps_to_fused_stages[input_step]
      fused_stage.add_step(original_step)
      steps_to_fused_stages[original_step] = fused_stage
      # TODO: add original step -> new step mapping.
      # TODO: add dependencies via WatermarkNode.
    elif isinstance(original_step, GroupByKeyOnlyStep):
      # hallucinate a shuffle write and a shuffle read.

 
    # TODO: handle GroupByKeyStep.
    for unused_tag, pcoll_node in original_step.outputs.iteritems():
      for consumer_step in pcoll_node.consumers:
        if consumer_step not in seen:
          to_process.put(consumer_step)
          seen.add(consumer_step)

  # Add fused stages to graph.
  for fused_stage in set(steps_to_fused_stages.values()):
    fused_stage.finalize_steps()
    execution_graph.add_stage(fused_stage)

  return execution_graph



class LaserRunner(PipelineRunner):
  """Executes a pipeline using multiple processes on the local machine."""

  def __init__(self):
    self.step_graph = StepGraph()

  def run_Read(self, transform_node):
    self._run_read_from(transform_node, transform_node.transform.source)

  def _run_read_from(self, transform_node, source_input):
    """Used when this operation is the result of reading source."""
    if not isinstance(source_input, NativeSource):
      source_bundle = iobase.SourceBundle(1.0, source_input, None, None)
    else:
      source_bundle = source_input
    print 'source', source_bundle
    # print 'split off', list(source_bundle.source.split(1))
    output = transform_node.outputs[None]
    element_coder = self._get_coder(output)
    
    # step_info = StepInfo(None, transform_node.outputs)
    step = ReadStep(transform_node.full_label, source_bundle, element_coder)
    # print 'transform_node', transform_node.outputs[None].producer
    self.step_graph.add_step(transform_node, step)
    print 'READ STEP', step

    # read_op = operation_specs.WorkerRead(source, output_coders=[element_coder])
    # print 'READ OP', read_op
    # self.outputs[output] = len(self.map_tasks), 0, 0
    # self.map_tasks.append([(transform_node.full_label, read_op)])
    # return len(self.map_tasks) - 1

  def _get_coder(self, pvalue, windowed=True):
    # TODO(robertwb): This should be an attribute of the pvalue itself.
    return DataflowRunner._get_coder(
        pvalue.element_type or typehints.Any,
        pvalue.windowing.windowfn.get_window_coder() if windowed else None)

  # def apply__GroupByKeyOnly(self, transform, pcoll):
  #   return pvalue.PCollection(pcoll.pipeline)

  def run__GroupByKeyOnly(self, transform_node):
    output = transform_node.outputs[None]
    element_coder = self._get_coder(output)
    step = GroupByKeyOnlyStep(transform_node.full_label, element_coder)
    print 'GBKO STEP', step
    self.step_graph.add_step(transform_node, step)




    # input_tag = transform_node.inputs[0].tag
    # input_step = self._cache.get_pvalue(transform_node.inputs[0])
    # step = self._add_step(
    #     TransformNames.GROUP, transform_node.full_label, transform_node)
    # step.add_property(
    #     PropertyNames.PARALLEL_INPUT,
    #     {'@type': 'OutputReference',
    #      PropertyNames.STEP_NAME: input_step.proto.name,
    #      PropertyNames.OUTPUT_NAME: input_step.get_output(input_tag)})
    # step.encoding = self._get_encoded_output_coder(transform_node)
    # step.add_property(
    #     PropertyNames.OUTPUT_INFO,
    #     [{PropertyNames.USER_NAME: (
    #         '%s.%s' % (transform_node.full_label, PropertyNames.OUT)),
    #       PropertyNames.ENCODING: step.encoding,
    #       PropertyNames.OUTPUT_NAME: PropertyNames.OUT}])
    # windowing = transform_node.transform.get_windowing(
    #     transform_node.inputs)
    # step.add_property(
    #     PropertyNames.SERIALIZED_FN,
    #     self.serialize_windowing_strategy(windowing))


  def run_ParDo(self, transform_node):
    transform = transform_node.transform
    output = transform_node.outputs[None]
    element_coder = self._get_coder(output)
    pardo_fn_data = ParDoFnData(*DataflowRunner._pardo_fn_data(
            transform_node,
            lambda side_input: self.side_input_labels[side_input]))  # TODO
    print 'output_tags', transform.output_tags  # TODO once we have multiple outputs or whatever
    print 'pardo_fn_data', pardo_fn_data
    print 'node', transform_node
    print 'input node', transform_node.inputs[0]
    print 'OUTPUT TAGS', transform.output_tags
    step = ParDoStep(transform_node.full_label, pardo_fn_data, element_coder, transform.output_tags)
    print 'PARDO STEP', step

    self.step_graph.add_step(transform_node, step)

  def run(self, pipeline):
    # Visit the pipeline and build up the step graph.
    super(LaserRunner, self).run(pipeline)

    print 'COMPLETEd step graph', self.step_graph
    for step in self.step_graph.steps:
      print step.inputs, step.outputs

    node_manager = InProcessComputeNodeManager()
    node_manager.start()
    work_manager = WorkManager(node_manager)
    watermark_manager = WatermarkManager()
    execution_context = ExecutionContext(work_manager, watermark_manager)
    print 'GENERATING EXECUTION GRAPH'
    execution_graph = generate_execution_graph(self.step_graph, execution_context)

    print 'EXECUTION GRAPH', execution_graph
    print execution_graph.stages
    for stage in execution_graph.stages:
      execution_context.watermark_manager.track_nodes(stage)
    print 'ROOT NODES', execution_context.watermark_manager.root_nodes
    executor = Executor(execution_graph, execution_context)
    executor.run()




# class CoordinatorInterface(Interface):

#   @remote_method(int, str)
#   def register_worker(host_id, worker_id):
#     raise NotImplementedError()

#   @remote_method(str, returns=str)
#   def ping(self, body):
#     raise NotImplementedError()

# class LaserCoordinator(threading.Thread, CoordinatorInterface):

#   def __init__(self):
#     self.channel_manager = get_channel_manager()
#     self.worker_channels = {}
#     super(LaserCoordinator, self).__init__()

#   def run(self):
#     print 'RUN'
#     while True:
#       for worker_id in self.worker_channels:
#         start_time = time.time()
#         for i in range(100):
#           self.worker_channels[worker_id].ping()
#         end_time = time.time()
#         print 'WORKER PING', worker_id, (end_time - start_time) / 100
#       time.sleep(1)


#   def register_worker(self, host_id, worker_id):
#     print 'REGISTERED', host_id, worker_id
#     # self.worker_channels[worker_id] = self.channel_manager.get_interface(HostDescriptor(host_id), 'worker', WorkerInterface)


#   def ping(self, body):
#     # print 'LaserCoordinator PINGED', body
#     return 'PINGED_%r' % body

class WorkerInterface(Interface):

  @remote_method(returns=str)
  def ping(self):
    raise NotImplementedError()

  @remote_method(object)
  def schedule_work_item(self, work_item):
    raise NotImplementedError()


class LaserWorker(WorkerInterface, threading.Thread):
  def __init__(self, config):
    super(LaserWorker, self).__init__()
    self.config = config
    self.worker_id = config['name']

    self.channel_manager = get_channel_manager()
    self.work_manager = self.channel_manager.get_interface('master/work_manager', WorkManagerInterface)

    self.lock = threading.Lock()
    self.current_work_item = None
    self.new_work_condition = threading.Condition(self.lock)

  def ping(self):
    return 'OK'

  def run(self):
    self.channel_manager.register_interface('%s/worker' % self.worker_id, self)
    self.work_manager.register_worker(self.worker_id)
    while True:
      with self.lock:
        while not self.current_work_item:
          self.new_work_condition.wait()
        work_item = self.current_work_item
      counter_factory = CounterFactory()
      state_sampler = statesampler.StateSampler(work_item.stage_name, counter_factory)
      map_executor = operations.SimpleMapTaskExecutor(work_item.map_task, counter_factory, state_sampler)
      status = WorkItemStatus.COMPLETED
      try:
        print '>>>>>>>>> WORKER', self.worker_id, 'EXECUTING WORK ITEM', work_item.id, work_item.map_task
        map_executor.execute()
        print '<<<<<<<<< WORKER DONE', self.worker_id, 'EXECUTING WORK ITEM', work_item.id, work_item.map_task
      except Exception as e:
        print 'Exception while processing work:', e
        status = WorkItemStatus.FAILED
      with self.lock:
        self.current_work_item = None
      self.work_manager.report_work_status(self.worker_id, work_item.id, status)



  # REMOTE METHOD
  def schedule_work_item(self, work_item):
    with self.lock:
      if self.current_work_item:
        raise Exception('Currently executing work item: %s' % self.current_work_item)
      self.current_work_item = work_item
      self.new_work_condition.notify()






class WorkItem(object):
  def __init__(self, id, stage_name, map_task):
    self.id = id
    self.stage_name = stage_name
    self.map_task = map_task
    # TODO: attempt number


class WorkItemStatus(object):
  NOT_STARTED = 0
  RUNNING = 1
  COMPLETED = 2
  FAILED = 3


# TODO: some work item progress / execution info

class WorkManagerInterface(Interface):
  @remote_method(int, int)
  def report_work_status(self, work_item_id, new_status):
    raise NotImplementedError()

  @remote_method(str)
  def register_worker(self, worker_id):
    pass


class WorkManager(WorkManagerInterface, threading.Thread):  # TODO: do we need a separate worker pool manager?
  def __init__(self, node_manager):
    super(WorkManager, self).__init__()
    self.channel_manager = get_channel_manager()

    self.node_manager = node_manager
    self.work_items = {}
    self.unscheduled_work = Queue()  # TODO: should this just be a set?

    self.work_status = {}
    self.work_stage = {}
    self.lock = threading.Lock()
    self.event_condition = threading.Condition(self.lock)

    self.worker_interfaces = {}
    self.all_workers = set()
    self.idle_workers = set()
    self.active_workers = set()

  def schedule_map_task(self, stage, map_task):  # TODO: should we track origin?  (yes)
    with self.lock:
      work_item_id = len(self.work_items)
      print 'SCHEDULE_MAP_TASK', stage, map_task, work_item_id
      work_item = WorkItem(work_item_id, stage.name, map_task)
      self.work_items[work_item_id] = work_item
      self.work_status[work_item] = WorkItemStatus.NOT_STARTED
      self.work_stage[work_item] = stage
      self.unscheduled_work.put(work_item)
      self.event_condition.notify()
    return work_item_id

  def run(self):
    print 'WORK MANAGER STARTING'
    self.channel_manager.register_interface('master/work_manager', self)
    print 'STARTING COMPUTE NODES'
    for node_stub in self.node_manager.get_nodes():
      while True:
        try:
          print 'START COMPUTE', node_stub, node_stub.start_worker()
          break
        except InterfaceNotReadyException:
          print 'NOT READYU'
          pass
    while True:
      print 'WORK MANAGER POLL', self.unscheduled_work, self.idle_workers
      keep_scheduling = True
      to_execute = []
      while keep_scheduling:
        with self.lock:
          work_item = None
          worker_interface = None
          if not self.unscheduled_work.empty() and self.idle_workers:
            work_item = self.unscheduled_work.get()
            worker_id = self.idle_workers.pop()
            self.active_workers.add(worker_id)
            worker_interface = self.worker_interfaces[worker_id]
            to_execute.append((worker_interface, work_item))
            self.work_status[work_item] = WorkItemStatus.RUNNING  # TODO: do we want more granular status?
          else:
            keep_scheduling = False
      print 'SCHEDULING', to_execute
      for worker_interface, work_item in to_execute:
        worker_interface.schedule_work_item(work_item)

      with self.lock:
        if not self.unscheduled_work.empty() and self.idle_workers:
          continue
        else:
          self.event_condition.wait()

  # REMOTE METHOD
  def register_worker(self, worker_id):
    print '********************REGISTER WORKER', worker_id
    with self.lock:
      # TODO: get a better worker wrapper class.
      worker_interface = self.channel_manager.get_interface('%s/worker' % worker_id, WorkerInterface)
      self.worker_interfaces[worker_id] = worker_interface
      self.all_workers.add(worker_id)
      self.idle_workers.add(worker_id)
      self.event_condition.notify()

  # REMOTE METHOD
  def report_work_status(self, worker_id, work_item_id, new_status):
    # TODO: generation id / attempt number
    with self.lock:
      # TODO: some validation
      work_item = self.work_items[work_item_id]
      assert self.work_status[work_item] == WorkItemStatus.RUNNING
      if new_status == WorkItemStatus.COMPLETED:
        self.work_status[work_item] = WorkItemStatus.COMPLETED
      elif new_status == WorkItemStatus.FAILED:
        self.work_status[work_item] = WorkItemStatus.FAILED
      else:
        raise ValueError('Invalid WorkItemStatus: %d' % new_status)
    if new_status == WorkItemStatus.COMPLETED:
      self.work_stage[work_item].report_work_completion(work_item_id)
    elif new_status == WorkItemStatus.FAILED:
      # TODO: retry, failure count
      with self.lock:
        self.work_status[work_item] = WorkItemStatus.NOT_STARTED
        self.unscheduled_work.put(work_item)
    with self.lock:
      self.active_workers.remove(worker_id)
      self.idle_workers.add(worker_id)
      self.event_condition.notify()
    # worker = self.active_workers[worker_id]
    # del self.active_workers[worker_id]





class ComputeNodeManagerInterface(Interface):
  pass

class ComputeNodeManager(ComputeNodeManagerInterface):
  # def supports_scaling(self):
  #   return False

  def _check_started(self):
    if not self.started:
      raise Exception('Node manager not started.')
  @remote_method(str, returns=str)
  def report_node_started(self, node_name):
    raise NotImplementedError()

# class ComputeNodeHandle(object):
#   def __init__(self, manager, name, core_count):
#     self.manager = manager
#     self.name = name
#     self.core_count = core_count

#   def get_node_interface(self):
#     pass


class ComputeNodeStubInterface(Interface):
  
  @remote_method(str)
  def confirm(self, message):
    raise NotImplementedError

class ComputeNodeStub(ComputeNodeStubInterface):
  def __init__(self, node_config):
    # TODO: node_config should be a class or something, not just a dict
    self.config = node_config
    # TODO: id?
    self.name = node_config['name']
    self.channel_manager = get_channel_manager()
    self.channel_manager.register_interface('%s/stub' % self.name, self)
    self.node_manager = self.channel_manager.get_interface('master/node_manager', ComputeNodeManagerInterface)
    self.worker = None


  def start(self):
    time.sleep(0.1)  # TODO: wait for node manager / link to be ready.
    self.node_manager.report_node_started(self.name)

  # REMOTE METHOD
  def start_worker(self):
    # TODO: maybe in the future this will be another process
    self.worker = LaserWorker(self.config)
    self.worker.start()
    return 'wtfwtfwtf'


  def confirm(self, message):
    print 'CONFIRMed', self.name, message


class InProcessComputeNodeManager(ComputeNodeManager):

  def __init__(self, num_nodes=4):
    self.num_nodes = num_nodes
    self.started = False
    self.channel_manager = get_channel_manager()
    self.channel_manager.register_interface('master/node_manager', self)

    self.node_stubs = {}


  def start(self):
    for i in range(self.num_nodes):
      worker_name = 'worker%d' % i
      self.channel_manager.register_node_address(worker_name)
      node_stub = ComputeNodeStub({'name': worker_name})
      threading.Thread(target=node_stub.start).start()
      self.node_stubs[worker_name] = node_stub
    self.started = True

  def report_node_started(self, node_name):
    # TODO: assert node name is correct
    # TODO: what happens if node restarts?
    print 'NODE STARTED', node_name
    node_stub = self.channel_manager.get_interface('%s/stub' % node_name, ComputeNodeStubInterface)
    node_stub.confirm('yay' + str(self))
    return 'HI[%s]' % node_name


  def get_nodes(self):
    self._check_started()
    return list(self.node_stubs.values())




def spawn_worker(options):
  p = subprocess.Popen(['python', '-m', 'apache_beam.runners.laser.laser_runner', '--worker', json.dumps(options)])




def run(argv):
  if '--worker' in argv:
    worker = LaserWorker(json.loads(argv[-1]))
    worker.run()
    sys.exit(0)

  COORDINATOR_PORT = random.randint(20000,30000)
  set_channel_config(ChannelConfig(
    node_addresses=['master'],
    link_strategies=[
      LinkStrategy(
        LinkStrategyType.LISTEN,
        mode=LinkMode.TCP,
        address='localhost',
        port=COORDINATOR_PORT,
        # mode=LinkMode.UNIX,
        # address='./uds_socket3-%s' % COORDINATOR_PORT,
        )]))
  manager = get_channel_manager()

  coordinator = LaserCoordinator()
  manager.register_interface('master/coordinator', coordinator)
  coordinator.start()

  print 'spawning worker'
  spawn_worker({
    'id': 'worker1',
    'channel_config': ChannelConfig(
    node_addresses=['worker1'],
    anycast_aliases = {'worker[any]': 'worker1'},
    link_strategies=[
      LinkStrategy(
        LinkStrategyType.CONNECT,
        mode=LinkMode.TCP,
        address='localhost',
        port=COORDINATOR_PORT,
        # mode=LinkMode.UNIX,
        # address='./uds_socket3-%s' % COORDINATOR_PORT,
        )]).to_dict(),
    })
  print 'DONE'
  # spawn_worker({
  #   'id': 102,
  #   'coordinator_host_id': manager.host_descriptor.host_id,
  #   'channel_config': {
  #     'host_id': 102,  # TODO: redundant
  #     'connect': True,
  #     'connect_mode': LinkMode.UNIX,
  #     'connect_address': './uds_socket3-%s' % COORDINATOR_PORT,
  #   }})
  # print 'DONE'
  time.sleep(10)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  set_channel_config(ChannelConfig(
    node_addresses=['master']))
  # InProcessComputeNodeManager().start()
  # raise ''
  # run(sys.argv)
  from apache_beam import Pipeline
  from apache_beam import Create
  from apache_beam import DoFn
  p = Pipeline(runner=LaserRunner())
  # def fn(input):
  #   print input
  p | Create([1, 2, 3]) | beam.Map(lambda x: (x, '1')) | beam.GroupByKey()
  # p | 'WTF' >> Create([1, 2, 3]) | 'm' >> beam.Map(lambda x: (x, '1'))
  # a = p | 'yo' >> Create(['a', 'b', 'c'])
  # def _print(x):
  #   print 'PRRRINT:', x
  # a | 'aaa' >> beam.Map(lambda x: (x, '2')) | 'bbb' >> beam.Map(lambda x: (x, '3')) | 'cc' >> beam.Map(_print)
  # a | beam.Map(lambda x: (x, '1'))# |  'gbk2' >>  beam.GroupByKey() | 'ccc' >> beam.Map(_print)
  # p | ReadFromText('gs://dataflow-samples/shakespeare/kinglear.txt') | beam.Map(lambda x: x.upper()) | beam.Map(_print)
  # # | beam.Map(fn)
  p.run()

