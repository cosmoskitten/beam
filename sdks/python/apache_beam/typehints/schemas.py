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

from typing import List
from typing import Mapping
from typing import NamedTuple
from typing import Optional
from uuid import uuid4

import numpy as np

from apache_beam.portability.api import schema_pb2
from apache_beam.typehints.native_type_compatibility import _get_args
from apache_beam.typehints.native_type_compatibility import _match_is_exactly_mapping
from apache_beam.typehints.native_type_compatibility import _match_is_named_tuple
from apache_beam.typehints.native_type_compatibility import _match_is_optional
from apache_beam.typehints.native_type_compatibility import _safe_issubclass
from apache_beam.typehints.native_type_compatibility import extract_optional_type


# Registry of typings for a schema by UUID
class SchemaTypeRegistry(object):
  def __init__(self):
    self.by_id = {}
    self.by_typing = {}

  def add(self, typing, schema):
    self.by_id[schema.id] = (typing, schema)

  def get_typing_by_id(self, unique_id):
    result = self.by_id.get(unique_id, None)
    return result[0] if result is not None else None

  def get_schema_by_id(self, unique_id):
    result = self.by_id.get(unique_id, None)
    return result[1] if result is not None else None


SCHEMA_REGISTRY = SchemaTypeRegistry()


_PRIMITIVES = (
    (np.int8, schema_pb2.AtomicType.BYTE),
    (np.int16, schema_pb2.AtomicType.INT16),
    (np.int32, schema_pb2.AtomicType.INT32),
    (np.int64, schema_pb2.AtomicType.INT64),
    (np.float32, schema_pb2.AtomicType.FLOAT),
    (np.float64, schema_pb2.AtomicType.DOUBLE),
    (np.unicode, schema_pb2.AtomicType.STRING),
    (np.bool, schema_pb2.AtomicType.BOOLEAN),
    (np.bytes_, schema_pb2.AtomicType.BYTES),
)

PRIMITIVE_TO_ATOMIC_TYPE = dict((typ, atomic) for typ, atomic in _PRIMITIVES)
ATOMIC_TYPE_TO_PRIMITIVE = dict((atomic, typ) for typ, atomic in _PRIMITIVES)


def typing_to_runner_api(type_):
  if _match_is_named_tuple(type_):
    schema = None
    if hasattr(type_, 'id'):
      schema = SCHEMA_REGISTRY.get_schema_by_id(type_.id)
    if schema is None:
      fields = [
          schema_pb2.Field(
              name=name, type=typing_to_runner_api(type_._field_types[name]))
          for name in type_._fields
      ]
      type_id = str(uuid4())
      schema = schema_pb2.Schema(fields=fields, id=type_id)
      SCHEMA_REGISTRY.add(type_, schema)

    return schema_pb2.FieldType(
        row_type=schema_pb2.RowType(
            schema=schema))

  elif _safe_issubclass(type_, List):
    element_type = typing_to_runner_api(_get_args(type_)[0])
    return schema_pb2.FieldType(
        array_type=schema_pb2.ArrayType(element_type=element_type))

  elif _match_is_exactly_mapping(type_):
    key_type, value_type = map(typing_to_runner_api, _get_args(type_))
    return schema_pb2.FieldType(
        map_type=schema_pb2.MapType(key_type=key_type, value_type=value_type))

  elif _match_is_optional(type_):
    # It's possible that a user passes us Optional[Optional[T]], but in python
    # typing this is indistinguishable from Optional[T] - both resolve to
    # Union[T, None] - so there's no need to check for that case here.
    result = typing_to_runner_api(extract_optional_type(type_))
    result.nullable = True
    return result
  # Remaining options should be a (primitive) type
  elif type(type_) is type:
    try:
      result = PRIMITIVE_TO_ATOMIC_TYPE[type_]
    except KeyError:
      raise ValueError(
          "Encountered unexpected primitive type: {0}".format(type_))
    return schema_pb2.FieldType(atomic_type=result)


def typing_from_runner_api(fieldtype_proto):
  if fieldtype_proto.nullable:
    base_type = schema_pb2.FieldType()
    base_type.CopyFrom(fieldtype_proto)
    base_type.nullable = False
    return Optional[typing_from_runner_api(base_type)]

  type_info = fieldtype_proto.WhichOneof("type_info")
  if type_info == "atomic_type":
    try:
      return ATOMIC_TYPE_TO_PRIMITIVE[fieldtype_proto.atomic_type]
    except KeyError:
      raise ValueError("Unsupported atomic type: {0}".format(
          fieldtype_proto.atomic_type))
  elif type_info == "array_type":
    return List[typing_from_runner_api(fieldtype_proto.array_type.element_type)]
  elif type_info == "map_type":
    return Mapping[
        typing_from_runner_api(fieldtype_proto.map_type.key_type),
        typing_from_runner_api(fieldtype_proto.map_type.value_type)
    ]
  elif type_info == "row_type":
    schema = fieldtype_proto.row_type.schema
    user_type = SCHEMA_REGISTRY.get_typing_by_id(schema.id)
    if user_type is None:
      type_name = 'BeamSchema_{}'.format(schema.id.replace('-', '_'))
      user_type = NamedTuple(type_name,
                             [(field.name, typing_from_runner_api(field.type))
                              for field in schema.fields])
      user_type.id = schema.id
      SCHEMA_REGISTRY.add(user_type, schema)
    return user_type

  elif type_info == "logical_type":
    pass  # TODO


def named_tuple_from_schema(schema):
  return typing_from_runner_api(
      schema_pb2.FieldType(row_type=schema_pb2.RowType(schema=schema)))


def named_tuple_to_schema(named_tuple):
  return typing_to_runner_api(named_tuple).row_type.schema
