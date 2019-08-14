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

"""Defines Transform whose expansion is implemented elsewhere.

No backward compatibility guarantees. Everything in this module is experimental.
"""
from __future__ import absolute_import
from __future__ import print_function

import contextlib
import copy
import threading

from apache_beam import pvalue
from apache_beam import typehints
from apache_beam.coders import registry
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_expansion_api_pb2
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import pipeline_context
from apache_beam.portability.api.external_transforms_pb2 import ConfigValue
from apache_beam.portability.api.external_transforms_pb2 import ExternalConfigurationPayload
from apache_beam.transforms import ptransform

# Protect against environments where grpc is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  import grpc
  from apache_beam.portability.api import beam_expansion_api_pb2_grpc
except ImportError:
  grpc = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

DEFAULT_EXPANSION_SERVICE = 'localhost:8097'


def iter_urns(coder, context=None):
  yield coder.to_runner_api_parameter(context)[0]
  for child in coder._get_component_coders():
    for urn in iter_urns(child, context):
      yield urn


class PayloadBuilder(object):
  """
  Abstract base class for building payloads to pass to ExternalTransform.
  """

  @classmethod
  def _config_value(cls, obj, typehint=None):
    """
    Helper to create a ConfigValue with an encoded value.
    """
    if typehint is None:
      coder = registry.get_coder(type(obj))
    else:
      coder = registry.get_coder(typehint)
    return ConfigValue(
      coder_urn=list(iter_urns(coder)),
      payload=coder.encode(obj))

  def build(self):
    """
    :return: ExternalConfigurationPayload
    """
    raise NotImplementedError


class SchemaBasedPayloadBuilder(PayloadBuilder):
  """
  Base class for building payloads based on a schema that provides
  type information for each configuration value to encode.
  """

  def __init__(self, values, schema=None):
    self._values = values
    self._schema = schema or {}

  @classmethod
  def _encode_config(cls, config, schema):
    result = {}
    for k, v in config.items():
      typehint = schema.get(k)
      if v is None and (
          typehint is None or not isinstance(typehint, typehints.Optional)):
        # make it easy for user to filter None by default
        continue
      result[k] = cls._config_value(v, typehint)
    return result

  def build(self):
    args = self._encode_config(self._values, self._schema)
    return ExternalConfigurationPayload(configuration=args)


class NamedTupleBasedPayloadBuilder(SchemaBasedPayloadBuilder):
  """
  Build a payload based on a NamedTuple schema.
  """
  def __init__(self, tuple_instance):
    super(NamedTupleBasedPayloadBuilder, self).__init__(
      tuple_instance._field_types, tuple_instance._asdict())


class AnnotationBasedPayloadBuilder(SchemaBasedPayloadBuilder):
  """
  Build a payload based on an external transform's type annotations.

  Supported in python 3 only.
  """
  def __init__(self, transform, **values):
    schema = {k: v for k, v in
              transform.__init__.__annotations__.items()
              if k in self._values}
    super(AnnotationBasedPayloadBuilder, self).__init__(values, schema)


class DataclassBasedPayloadBuilder(SchemaBasedPayloadBuilder):
  """
  Build a payload based on an external transform that uses dataclasses.

  Supported in python 3 only.
  """
  def __init__(self, transform):
    import dataclasses
    schema = {field.name: field.type for field in
              dataclasses.fields(transform)}
    super(DataclassBasedPayloadBuilder, self).__init__(
      dataclasses.asdict(transform), schema)


class ExternalTransform(ptransform.PTransform):
  """
    External provides a cross-language transform via expansion services in
    foreign SDKs.

    Experimental; no backwards compatibility guarantees.
  """
  _namespace_counter = 0
  _namespace = threading.local()

  _EXPANDED_TRANSFORM_UNIQUE_NAME = 'root'
  _IMPULSE_PREFIX = 'impulse'

  def __init__(self, urn, payload, endpoint=None):
    endpoint = endpoint or DEFAULT_EXPANSION_SERVICE
    if grpc is None and isinstance(endpoint, str):
      raise NotImplementedError('Grpc required for external transforms.')
    # TODO: Start an endpoint given an environment?
    self._urn = urn
    self._payload = payload.build() if isinstance(payload, PayloadBuilder) \
      else payload
    self._endpoint = endpoint
    self._namespace = self._fresh_namespace()

  def default_label(self):
    return '%s(%s)' % (self.__class__.__name__, self._urn)

  @classmethod
  def get_local_namespace(cls):
    return getattr(cls._namespace, 'value', 'external')

  @classmethod
  @contextlib.contextmanager
  def outer_namespace(cls, namespace):
    prev = cls.get_local_namespace()
    cls._namespace.value = namespace
    yield
    cls._namespace.value = prev

  @classmethod
  def _fresh_namespace(cls):
    ExternalTransform._namespace_counter += 1
    return '%s_%d' % (cls.get_local_namespace(), cls._namespace_counter)

  def expand(self, pvalueish):
    if isinstance(pvalueish, pvalue.PBegin):
      self._inputs = {}
    elif isinstance(pvalueish, (list, tuple)):
      self._inputs = {str(ix): pvalue for ix, pvalue in enumerate(pvalueish)}
    elif isinstance(pvalueish, dict):
      self._inputs = pvalueish
    else:
      self._inputs = {'input': pvalueish}
    pipeline = (
        next(iter(self._inputs.values())).pipeline
        if self._inputs
        else pvalueish.pipeline)
    context = pipeline_context.PipelineContext()
    transform_proto = beam_runner_api_pb2.PTransform(
        unique_name=self._EXPANDED_TRANSFORM_UNIQUE_NAME,
        spec=beam_runner_api_pb2.FunctionSpec(
            urn=self._urn, payload=self._payload))
    for tag, pcoll in self._inputs.items():
      transform_proto.inputs[tag] = context.pcollections.get_id(pcoll)
      # Conversion to/from proto assumes producers.
      # TODO: Possibly loosen this.
      context.transforms.put_proto(
          '%s_%s' % (self._IMPULSE_PREFIX, tag),
          beam_runner_api_pb2.PTransform(
              unique_name='%s_%s' % (self._IMPULSE_PREFIX, tag),
              spec=beam_runner_api_pb2.FunctionSpec(
                  urn=common_urns.primitives.IMPULSE.urn),
              outputs={'out': transform_proto.inputs[tag]}))
    components = context.to_runner_api()
    request = beam_expansion_api_pb2.ExpansionRequest(
        components=components,
        namespace=self._namespace,
        transform=transform_proto)

    if isinstance(self._endpoint, str):
      with grpc.insecure_channel(self._endpoint) as channel:
        response = beam_expansion_api_pb2_grpc.ExpansionServiceStub(
            channel).Expand(request)
    else:
      response = self._endpoint.Expand(request, None)

    if response.error:
      raise RuntimeError(response.error)
    self._expanded_components = response.components
    self._expanded_transform = response.transform
    result_context = pipeline_context.PipelineContext(response.components)

    def fix_output(pcoll, tag):
      pcoll.pipeline = pipeline
      pcoll.tag = tag
      return pcoll
    self._outputs = {
        tag: fix_output(result_context.pcollections.get_by_id(pcoll_id), tag)
        for tag, pcoll_id in self._expanded_transform.outputs.items()
    }

    return self._output_to_pvalueish(self._outputs)

  def _output_to_pvalueish(self, output_dict):
    if len(output_dict) == 1:
      return next(iter(output_dict.values()))
    else:
      return output_dict

  def to_runner_api_transform(self, context, full_label):
    pcoll_renames = {}
    renamed_tag_seen = False
    for tag, pcoll in self._inputs.items():
      if tag not in self._expanded_transform.inputs:
        if renamed_tag_seen:
          raise RuntimeError(
              'Ambiguity due to non-preserved tags: %s vs %s' % (
                  sorted(self._expanded_transform.inputs.keys()),
                  sorted(self._inputs.keys())))
        else:
          renamed_tag_seen = True
          tag, = self._expanded_transform.inputs.keys()
      pcoll_renames[self._expanded_transform.inputs[tag]] = (
          context.pcollections.get_id(pcoll))
    for tag, pcoll in self._outputs.items():
      pcoll_renames[self._expanded_transform.outputs[tag]] = (
          context.pcollections.get_id(pcoll))

    def _equivalent(coder1, coder2):
      return coder1 == coder2 or _normalize(coder1) == _normalize(coder2)

    def _normalize(coder_proto):
      normalized = copy.copy(coder_proto)
      normalized.spec.environment_id = ''
      # TODO(robertwb): Normalize components as well.
      return normalized

    for id, proto in self._expanded_components.coders.items():
      if id.startswith(self._namespace):
        context.coders.put_proto(id, proto)
      elif id in context.coders:
        if not _equivalent(context.coders._id_to_proto[id], proto):
          raise RuntimeError('Re-used coder id: %s\n%s\n%s' % (
              id, context.coders._id_to_proto[id], proto))
      else:
        context.coders.put_proto(id, proto)
    for id, proto in self._expanded_components.windowing_strategies.items():
      if id.startswith(self._namespace):
        context.windowing_strategies.put_proto(id, proto)
    for id, proto in self._expanded_components.environments.items():
      if id.startswith(self._namespace):
        context.environments.put_proto(id, proto)
    for id, proto in self._expanded_components.pcollections.items():
      id = pcoll_renames.get(id, id)
      if id not in context.pcollections._id_to_obj.keys():
        context.pcollections.put_proto(id, proto)

    for id, proto in self._expanded_components.transforms.items():
      if id.startswith(self._IMPULSE_PREFIX):
        # Our fake inputs.
        continue
      assert id.startswith(self._namespace), (id, self._namespace)
      new_proto = beam_runner_api_pb2.PTransform(
          unique_name=full_label + proto.unique_name[
              len(self._EXPANDED_TRANSFORM_UNIQUE_NAME):],
          spec=proto.spec,
          subtransforms=proto.subtransforms,
          inputs={tag: pcoll_renames.get(pcoll, pcoll)
                  for tag, pcoll in proto.inputs.items()},
          outputs={tag: pcoll_renames.get(pcoll, pcoll)
                   for tag, pcoll in proto.outputs.items()})
      context.transforms.put_proto(id, new_proto)

    return beam_runner_api_pb2.PTransform(
        unique_name=full_label,
        spec=self._expanded_transform.spec,
        subtransforms=self._expanded_transform.subtransforms,
        inputs=self._expanded_transform.inputs,
        outputs={
            tag: pcoll_renames.get(pcoll, pcoll)
            for tag, pcoll in self._expanded_transform.outputs.items()})


def memoize(func):
  cache = {}

  def wrapper(*args):
    if args not in cache:
      cache[args] = func(*args)
    return cache[args]
  return wrapper
