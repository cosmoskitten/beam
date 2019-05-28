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

from apache_beam.io.restriction_trackers import SDFBoundedSourceRestrictionTracker
from apache_beam.transforms.core import RestrictionProvider

__all__ = ['SDFBoundedSourceRestrictionProvider']


class SDFBoundedSourceRestrictionProvider(RestrictionProvider):
  """A `RestrictionProvider` that is used by SDF for `BoundedSource`."""

  def __init__(self, source, desired_chunk_size=None):
    self._source = source
    self._desired_chunk_size = desired_chunk_size
    # The size cannot be calculated directly by end_pos - start_pos since the
    # position may not be numeric(e.g., key space position).
    self._restriction_size_map = {}

  def initial_restriction(self, element):
    # Get initial range_tracker from source
    range_tracker = self._source.get_range_tracker(None, None)
    return (range_tracker.start_position(), range_tracker.stop_position())

  def create_tracker(self, restriction):
    return SDFBoundedSourceRestrictionTracker(
        self._source.get_range_tracker(restriction[0],
                                       restriction[1]))

  def split(self, element, restriction):
    # Invoke source.split to get initial splitting results.
    source_bundles = self._source.split(self._desired_chunk_size)
    for source_bundle in source_bundles:
      self._restriction_size_map[(source_bundle.start_position,
                                  source_bundle.stop_position)]\
        = source_bundle.weight
      yield (source_bundle.start_position, source_bundle.stop_position)

  def restriction_size(self, element, restriction):
    return self._restriction_size_map[(restriction[0], restriction[1])]
