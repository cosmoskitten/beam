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
Datastore testing functions.

For internal use only; no backwards-compatibility guarantees.
"""

from __future__ import absolute_import

import uuid
from builtins import range

# Protect against environments where datastore library is not available.
try:
  from apache_beam.io.gcp.datastore.v1new import types
except ImportError:
  pass


def create_entities(count, id_or_name=False):
  """Creates a list of entities with random keys."""
  if id_or_name:
    ids_or_names = [uuid.uuid4().int & ((1 << 63) - 1) for _ in range(count)]
  else:
    ids_or_names = [str(uuid.uuid4()) for _ in range(count)]

  keys = [types.Key(['EntityKind', x], project='project') for x in ids_or_names]
  return [types.Entity(key) for key in keys]


def create_client_entities(count, id_or_name=False):
  """Creates a list of client-style entities with random keys."""
  return [entity.to_client_entity()
          for entity in create_entities(count, id_or_name=id_or_name)]
