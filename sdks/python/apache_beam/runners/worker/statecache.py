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

"""TODO mxm"""
import collections
from threading import Lock

# (cache_token, value)


class StateCache(object):

  def __init__(self, max_entries):
    self._cache = LRUCache(max_entries)
    self._lock = Lock()

  def get(self, state_key, cache_tokens):
    #assert isinstance(state_key, StateKey.__class__)
    with self._lock:
      cache_entry = self._cache.get(state_key)
    if cache_entry:
      token, value = cache_entry
      return value if token in cache_tokens else None
    else:
      return None

  def put(self, state_key, cache_token, value):
    #assert isinstance(state_key, StateKey.__class__)
    with self._lock:
      return self._cache.put(state_key, (cache_token, value))

  def clear(self, state_key):
    #assert isinstance(state_key, StateKey.__class__)
    with self._lock:
      self._cache.clear(state_key)

  def __len__(self):
    return len(self._cache)


class LRUCache(object):

  def __init__(self, max_entries):
    self._maxEntries = max_entries
    self._cache = collections.OrderedDict()

  def get(self, key):
    value = self._cache.pop(key, None)
    if value:
      self._cache[key] = value
    return value

  def put(self, key, value):
    while len(self._cache) >= self._maxEntries:
      self._cache.popitem(last=False)
    self._cache[key] = value

  def clear(self, key):
    self._cache.pop(key, None)

  def __len__(self):
    return len(self._cache)


class CacheStateKey(object):

  def __init__(self, transform_id, state_id, window, key):
    self.transform_id = transform_id
    self.state_id = state_id
    self.window = window
    self.key = key

  def __eq__(self, other):
    return (isinstance(other, self.__class__) and
            other.transform_id == self.transform_id and
            other.state_id == self.state_id and
            other.window == self.window and
            other.key == self.key)

  def __hash__(self):
    return hash((self.transform_id, self.state_id, self.window, self.key))

  def __repr__(self):
    return ("CacheStateKey["
            "transform_id={},"
            "state_id={},"
            "window={},"
            "key={}]"
            .format(self.transform_id, self.state_id, self.window, self.key))
