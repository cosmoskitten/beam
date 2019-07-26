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

import itertools
from array import array

from apache_beam.coders.coder_impl import StreamCoderImpl
from apache_beam.coders.coders import BytesCoder
from apache_beam.coders.coders import Coder
from apache_beam.coders.coders import FastCoder
from apache_beam.coders.coders import FloatCoder
from apache_beam.coders.coders import IterableCoder
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.coders.coders import TupleCoder
from apache_beam.coders.coders import VarIntCoder
from apache_beam.portability import common_urns
from apache_beam.portability.api import schema_pb2
from apache_beam.typehints.schemas import named_tuple_from_schema
from apache_beam.typehints.schemas import named_tuple_to_schema

__all__ = ["RowCoder"]


class RowCoder(FastCoder):
  """ Coder for `typing.NamedTuple` instances.

  Implements the beam:coder:row:v1 standard coder spec.
  """

  def __init__(self, schema):
    self.schema = schema
    self.components = [
        coder_from_type(field.type) for field in self.schema.fields
    ]

  def _create_impl(self):
    return RowCoderImpl(self.schema, self.components)

  def is_deterministic(self):
    return all(c.is_deterministic() for c in self.components)

  def to_type_hint(self):
    return named_tuple_from_schema(self.schema)

  def as_cloud_object(self, coders_context=None):
    raise NotImplementedError("TODO")

  def __eq__(self, other):
    return type(self) == type(other) and self.schema == other.schema

  def __hash__(self):
    return hash((type(self), self.schema.SerializePartialToString()))

  def to_runner_api_parameter(self, unused_context):
    return (common_urns.coders.ROW.urn, self.schema, [])

  @staticmethod
  def from_type_hint(named_tuple_type, registry):
    return RowCoder(named_tuple_to_schema(named_tuple_type))


def coder_from_type(type_):
  type_info = type_.WhichOneof("type_info")
  if type_info == "atomic_type":
    if type_.atomic_type in (schema_pb2.AtomicType.INT32,
                             schema_pb2.AtomicType.INT64):
      return VarIntCoder()
    elif type_.atomic_type == schema_pb2.AtomicType.DOUBLE:
      return FloatCoder()
    elif type_.atomic_type == schema_pb2.AtomicType.STRING:
      return StrUtf8Coder()
  elif type_info == "array_type":
    return IterableCoder(coder_from_type(type_.array_type.element_type))

  # The Java SDK supports several more types, but the coders are not yet
  # standard, and are not implemented in Python.
  raise ValueError(
      "Encountered a type that is not currently supported by RowCoder: %s" %
      type_)


# pylint: disable=unused-variable


@Coder.register_urn(common_urns.coders.ROW.urn, schema_pb2.Schema)
def from_runner_api_parameter(payload, components, unused_context):
  return RowCoder(payload)


class RowCoderImpl(StreamCoderImpl):
  """For internal use only; no backwards-compatibility guarantees."""
  SIZE_CODER = VarIntCoder().get_impl()
  NULL_CODER = BytesCoder().get_impl()

  def __init__(self, schema, components):
    self.schema = schema
    self.constructor = named_tuple_from_schema(schema)
    self.components = components
    self.has_nullable_fields = any(
        field.type.nullable for field in self.schema.fields)

  def encode_to_stream(self, value, out, nested):
    nvals = len(self.schema.fields)
    self.SIZE_CODER.encode_to_stream(nvals, out, True)

    if self.has_nullable_fields and any(attr is None for attr in value):
      nulls = [attr is None for attr in value]
      words = array('B', itertools.repeat(0, (nvals+7)//8))
      index = 0
      for i, is_null in enumerate(nulls):
        words[i//8] |= is_null << (i % 8)
      value_coder = self._make_value_coder(nulls)
    else:
      words = array('B')
      value_coder = self._make_value_coder()

    self.NULL_CODER.encode_to_stream(words.tostring(), out, True)

    # TODO: inline tuple coder and null checks. reference by attribute name
    # rather than assuming tuple
    value_coder.encode_to_stream(
        tuple(attr for attr in value if attr is not None), out, nested)

  def decode_from_stream(self, in_stream, nested):
    # TODO: handle schema changes
    nvals = self.SIZE_CODER.decode_from_stream(in_stream, True)
    words = array('B')
    words.fromstring(self.NULL_CODER.decode_from_stream(in_stream, True))

    if words:
      nulls = [(words[i // 8] >> (i % 8)) & 0x01 for i in range(nvals)]
      value_coder = self._make_value_coder(nulls)
    else:
      nulls = None
      value_coder = self._make_value_coder()

    values = value_coder.decode_from_stream(in_stream, False)

    if nulls is None:
      expanded_values = values
    else:
      i = 0
      expanded_values = []
      for is_null in nulls:
        if is_null:
          expanded_values.append(None)
        else:
          expanded_values.append(values[i])
          i += 1

    return self.constructor(*expanded_values)

  def _make_value_coder(self, nulls=itertools.repeat(False)):
    components = [
        component for component, is_null in zip(self.components, nulls)
        if not is_null
    ] if self.has_nullable_fields else self.components
    return TupleCoder(components).get_impl()
