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

"""``PTransforms`` for manipulating files in Apache Beam.

Provides thre reading ``PTransform``\\s, ``MatchFiles``,
``MatchAll``, that produces a ``PCollection`` of records representing a file
and its metadata; and ``ReadAll``, which takes in a ``PCollection`` of file
metadata records, and produces a ``PCollection`` of (file metadata, file handle)
tuples.
These transforms currently do not support splitting by themselves.
"""

from __future__ import absolute_import

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import filesystem
from apache_beam.io import filesystems
from apache_beam.io.filesystem import BeamIOError


class EmptyMatchTreatment(object):
  ALLOW = 'ALLOW'
  DISALLOW = 'DISALLOW'
  ALLOW_IF_WILDCARD = 'ALLOW_IF_WILDCARD'

  @staticmethod
  def allow_empty_match(pattern, setting):
    if setting == EmptyMatchTreatment.ALLOW:
      return True
    elif setting == EmptyMatchTreatment.ALLOW_IF_WILDCARD and '*' in pattern:
      return True
    else:
      return False


class _MatchAllFn(beam.DoFn):

  def __init__(self, empty_match_treatment):
    self._empty_match_treatment = empty_match_treatment

  def process(self, file_pattern):
    # TODO: Should we batch the lookups?
    match_results = filesystems.FileSystems.match([file_pattern])
    match_result = match_results[0]

    if (not match_result.metadata_list
        and not EmptyMatchTreatment.allow_empty_match(
            file_pattern, self._empty_match_treatment)):
      raise BeamIOError(
          'Empty match for pattern %s. Disallowed.' % file_pattern)

    return match_result.metadata_list


class MatchFiles(beam.PTransform):

  def __init__(self, file_pattern, empty_match_treatment=None):
    self._file_pattern = file_pattern
    self._empty_match_treatment = (empty_match_treatment
                                   or EmptyMatchTreatment.ALLOW_IF_WILDCARD)

  def expand(self, pcoll):
    return (pcoll.pipeline
            | beam.Create([self._file_pattern])
            | MatchAll())


class MatchAll(beam.PTransform):

  def __init__(self, empty_match_treatment=None):
    self._empty_match_treatment = (empty_match_treatment
                                   or EmptyMatchTreatment.ALLOW)

  def expand(self, pcoll):
    return (pcoll
            | beam.ParDo(_MatchAllFn(self._empty_match_treatment)))


class _ReadMatchesFn(beam.DoFn):

  def __init__(self, compression, skip_directories):
    self._compression = compression
    self._skip_directories = skip_directories

  def process(self, file_metadata):
    metadata = (filesystem.FileMetadata(file_metadata, 0)
                if isinstance(file_metadata, (str, unicode))
                else file_metadata)

    if metadata.path.endswith('/') and self._skip_directories:
      return
    elif metadata.path.endswith('/'):
      raise BeamIOError(
          'Directories are not allowed in ReadMatches transform.'
          'Found %s.' % metadata.path)

    # TODO: Mime type? OTher stuff? Maybe arugments passed in to transform?
    yield ReadableFile(metadata)


class ReadableFile(object):

  def __init__(self, metadata):
    self.metadata = metadata

  def open(self):
    return filesystems.FileSystems.open(self.metadata.path)

  def read(self):
    return self.open().read()



class ReadMatches(beam.PTransform):

  def __init__(self, compression=None, skip_directories=True):
    self._compression = compression
    self._skip_directories = skip_directories

  def expand(self, pcoll):
    return pcoll | beam.ParDo(_ReadMatchesFn(self._compression,
                                             self._skip_directories))
