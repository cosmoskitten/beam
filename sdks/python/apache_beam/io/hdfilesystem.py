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

"""FileSystem implementation for accessing Hadoop Distributed File System
files."""

from __future__ import absolute_import

import posixpath

from hdfs3 import HDFileSystem as HDFS3

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem
from apache_beam.io.filesystem import MatchResult

__all__ = ['HdFileSystem']


COPY_BUFFER_SIZE = 2**16


# TODO(udim): Add @retry.with_exponential_backoff to some functions, like in
# gcsio.py.


class HdFileSystem(FileSystem):
  """FileSystem implementation that supports HDFS."""

  def __init__(self, *args, **kwargs):
    """Initialize a connection to HDFS.

    Keyword arguments:
      host: Hostname of HDFS server to connect to.
      port: Port of HDFS server to connect to.
    """
    host = kwargs.pop('host')
    port = kwargs.pop('port')
    super(HdFileSystem, self).__init__(*args, **kwargs)
    self._hdfs = HDFS3(host=host, port=port)

  @classmethod
  def scheme(cls):
    return 'hdfs'

  def join(self, basepath, *paths):
    return posixpath.join(basepath, *paths)

  def split(self, path):
    return posixpath.split(path)

  def mkdirs(self, path):
    if self.exists(path):
      raise IOError('%s: path already exists' % path)
    self._hdfs.makedirs(path)

  def match(self, patterns, limits=None):
    if limits is None:
      limits = [None] * len(patterns)
    else:
      err_msg = 'Patterns and limits should be equal in length'
      assert len(patterns) == len(limits), err_msg

    def _match(pattern, limit):
      """Find all matching paths to the pattern provided."""
      fileinfos = self._hdfs.ls(pattern, detail=True)[:limit]
      metadata_list = [FileMetadata(fileinfo['name'], fileinfo['size'])
                       for fileinfo in fileinfos]
      return MatchResult(pattern, metadata_list)

    exceptions = {}
    result = []
    for pattern, limit in zip(patterns, limits):
      try:
        result.append(_match(pattern, limit))
      except Exception as e:  # pylint: disable=broad-except
        exceptions[pattern] = e

    if exceptions:
      raise BeamIOError('Match operation failed', exceptions)
    return result

  def _open(self, path, mode, mime_type, compression_type):
    if mime_type != 'application/octet-stream':
      raise BeamIOError('Unsupported mime_type: %s', mime_type)
    if compression_type == CompressionTypes.AUTO:
      compression_type = CompressionTypes.detect_compression_type(path)
    res = self._hdfs.open(path, mode)
    if compression_type != CompressionTypes.UNCOMPRESSED:
      res = CompressedFile(res)
    return res

  def create(self, path, mime_type='application/octet-stream',
             compression_type=CompressionTypes.AUTO):
    return self._open(path, 'wb', mime_type, compression_type)

  def open(self, path, mime_type='application/octet-stream',
           compression_type=CompressionTypes.AUTO):
    return self._open(path, 'rb', mime_type, compression_type)

  def copy(self, source_file_names, destination_file_names):
    """Implements the FileSystem interface.

    Will overwrite files and directories in destination_file_names.
    """
    err_msg = ("source_file_names and destination_file_names should "
               "be equal in length")
    assert len(source_file_names) == len(destination_file_names), err_msg

    def _copy_file(source, destination):
      with self.open(source) as f1:
        with self.create(destination) as f2:
          while True:
            buf = f1.read(COPY_BUFFER_SIZE)
            if not buf:
              break
            f2.write(buf)

    def _copy_path(source, destination):
      """Recursively copy the file tree from the source to the destination."""
      if not self._hdfs.isdir(source):
        _copy_file(source, destination)
        return

      for path, dirs, files in self._hdfs.walk(source):
        for dir in dirs:
          new_dir = self.join(destination, dir)
          if not self.exists(new_dir):
            self.mkdirs(new_dir)

        relpath = posixpath.relpath(path, source)
        if relpath == '.':
          relpath = ''
        for file in files:
          _copy_file(self.join(path, file),
                     self.join(destination, relpath, file))

    exceptions = {}
    for source, destination in zip(source_file_names, destination_file_names):
      try:
        _copy_path(source, destination)
      except Exception as e:  # pylint: disable=broad-except
        exceptions[(source, destination)] = e

    if exceptions:
      raise BeamIOError("Copy operation failed", exceptions)

  def rename(self, source_file_names, destination_file_names):
    exceptions = {}
    for source, destination in zip(source_file_names, destination_file_names):
      try:
        if not self._hdfs.mv(source, destination):
          raise BeamIOError(
              'libhdfs error in rename(%s, %s)' % (source, destination))
      except Exception as e:  # pylint: disable=broad-except
        exceptions[(source, destination)] = e

    if exceptions:
      raise BeamIOError("Rename operation failed", exceptions)

  def exists(self, path):
    return self._hdfs.exists(path)

  def delete(self, paths):
    exceptions = {}
    for path in paths:
      try:
        self._hdfs.rm(path, recursive=True)
      except Exception as e:  # pylint: disable=broad-except
        exceptions[path] = e

    if exceptions:
      raise BeamIOError("Delete operation failed", exceptions)
