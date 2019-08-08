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

"""Module of the current iBeam (interactive Beam) environment.

Provides interfaces to manipulate existing iBeam environment. Internally used
by iBeam. External iBeam users please use interactive_beam module directly.
"""

import importlib

_ibeam_env = None


def current_env(cache_manager=None):
  """Gets current iBeam environment."""
  global _ibeam_env
  if not _ibeam_env:
    _ibeam_env = InteractiveEnvironment(cache_manager)
  return _ibeam_env


def new_env(cache_manager=None):
  """Creates a new iBeam environment to replace current one."""
  global _ibeam_env
  _ibeam_env = None
  return current_env(cache_manager)


class InteractiveEnvironment(object):
  """An interactive environment with cache and pipeline variable metadata.

  iBeam will use the watched variable information to determine if a PCollection
  is ever assigned to a variable in user pipeline when executing the pipeline
  and apply magic to automatically cache and use cache for those PCollections
  if the pipeline is executed within an interactive environment. Users can also
  visualize and introspect those PCollections in user code since they have
  handlers to the variables.
  """

  def __init__(self, cache_manager=None):
    self._cache_manager = cache_manager
    # Holds class instances, module object, string of module names.
    self._watching_set = set()
    # Holds variables list of (Dict[str, object]).
    self._watching_dict_list = []
    # Always watch __main__ module.
    self.watch('__main__')

  def watch(self, watchable):
    """Watches a watchable.

    A watchable can be a dictionary of variable metadata such as locals(), a str
    name of a module, a module object or an instance of a class. The variable
    can come from any scope even local. Duplicated variable naming doesn't
    matter since they are different instances. Duplicated variables are also
    allowed when watching.
    """
    if isinstance(watchable, dict):
      self._watching_dict_list.append(watchable.items())
    else:
      self._watching_set.add(watchable)

  def watching(self):
    watching = list(self._watching_dict_list)
    for watchable in self._watching_set:
      if isinstance(watchable, str):
        module = importlib.import_module(watchable)
        watching.append(vars(module).items())
      else:
        watching.append(vars(watchable).items())
    return watching

  def set_cache_manager(self, cache_manager):
    self._cache_manager = cache_manager

  def cache_manager(self):
    return self._cache_manager
