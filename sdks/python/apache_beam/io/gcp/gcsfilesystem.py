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
"""GCS File system for file based sources and sinks."""

from __future__ import absolute_import

from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileSystem
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.gcp import gcsio


class GCSFileSystem(FileSystem):
  """A GCS file system implementation for accessing files on disk.
  """

  def mkdirs(self, path):
    """Recursively create directories for the provided path.

    Args:
      path: string path of the directory structure that should be created

    Raises:
      IOError if leaf directory already exists.
    """
    pass

  def match(self, patterns, limits=None):
    """Find all matching paths to the pattern provided.

    Args:
      pattern: string for the file path pattern to match against
      limit: Maximum number of responses that need to be fetched

    Returns: list of list of ``FileMetadata`` objects that match the patterns.
    """
    if limits is None:
      limits = [None] * len(patterns)
    else:
      err_msg = "Patterns and limits should be equal in length"
      assert len(patterns) == len(limits), err_msg

    def _match(pattern, limit):
      """Find all matching paths to the pattern provided.
      """
      if pattern.endswith('/'):
        pattern += '*'
      file_sizes = gcsio.GcsIO().size_of_files_in_glob(pattern)
      return [FileMetadata(path, size) for path, size in file_sizes.iteritems()]
    return [_match(pattern, limit) for pattern, limit in zip(patterns, limits)]

  def _path_open(self, path, mode, mime_type='application/octet-stream',
                 compression_type=CompressionTypes.AUTO):
    """Helper functions to open a file in the provided mode.
    """
    compression_type = FileSystem._get_compression_type(path, compression_type)
    mime_type = CompressionTypes.mime_type(compression_type, mime_type)
    raw_file = gcsio.GcsIO().open(path, mode, mime_type=mime_type)
    if compression_type == CompressionTypes.UNCOMPRESSED:
      return raw_file
    else:
      return CompressedFile(raw_file, compression_type=compression_type)

  def create(self, path, mime_type='application/octet-stream',
             compression_type=CompressionTypes.AUTO):
    """Returns a write channel for the given file path.

    Args:
      path: string path of the file object to be written to the system
      mime_type: MIME type to specify the type of content in the file object
      compression_type: Type of compression to be used for this object

    Returns: file handle with a close function for the user to use
    """
    return GCSFileSystem._path_open(path, 'wb', mime_type, compression_type)

  def open(self, path, mime_type='application/octet-stream',
           compression_type=CompressionTypes.AUTO):
    """Returns a read channel for the given file path.

    Args:
      path: string path of the file object to be written to the system
      mime_type: MIME type to specify the type of content in the file object
      compression_type: Type of compression to be used for this object

    Returns: file handle with a close function for the user to use
    """
    return GCSFileSystem._path_open(path, 'rb', mime_type, compression_type)

  def copy(self, sources, destinations):
    """Recursively copy the file tree from the source to the destination

    Args:
      sources: list of source file/directory object that needs to be copied
      destinations: list of destination of the new object
    """
    err_msg = "Sources and Destinations should be equal in length"
    assert len(sources) == len(destinations), err_msg

    def _copy_path(source, destination):
      """Recursively copy the file tree from the source to the destination
      """
      if not destination.startswith('gs://'):
        raise ValueError('Destination %r must be GCS path.', destination)
      # Use copy_tree if the path ends with / as it is a directory
      if source.endswith('/'):
        gcsio.GcsIO().copytree(source, destination)
      else:
        gcsio.GcsIO().copy(source, destination)

    for source, destination in zip(sources, destinations):
      _copy_path(source, destination)

  def rename(self, sources, destinations):
    """Rename the files at the source list to the destination list.
    Source and destination lists should be of the same size.


    Args:
      sources: List of file paths that need to be moved
      destinations: List of respective destinations for the file objects

    Returns: list of exceptions encountered in the process
    """
    err_msg = "Sources and Destinations should be equal in length"
    assert len(sources) == len(destinations), err_msg

    gcs_batches = []
    gcs_current_batch = []
    for src, dest in zip(sources, destinations):
      gcs_current_batch.append((src, dest))
      if len(gcs_current_batch) == gcsio.MAX_BATCH_OPERATION_SIZE:
        gcs_batches.append(gcs_current_batch)
        gcs_current_batch = []
    if gcs_current_batch:
      gcs_batches.append(gcs_current_batch)

    # Execute GCS renames if any and return exceptions.
    exceptions = []
    for batch in gcs_batches:
      copy_statuses = gcsio.GcsIO().copy_batch(batch)
      copy_succeeded = []
      for src, dest, exception in copy_statuses:
        if exception:
          exceptions.append((src, dest, exception))
        else:
          copy_succeeded.append((src, dest))
      delete_batch = [src for src, dest in copy_succeeded]
      delete_statuses = gcsio.GcsIO().delete_batch(delete_batch)
      for i, (src, exception) in enumerate(delete_statuses):
        dest = copy_succeeded[i]
        if exception:
          exceptions.append((src, dest, exception))
    return exceptions

  def exists(self, path):
    """Check if the provided path exists on the FileSystem.

    Args:
      path: string path that needs to be checked.

    Returns: boolean flag indicating if path exists
    """
    return gcsio.GcsIO().exists(path)

  def delete(self, paths):
    """Recursively delete the file or directory at the provided path.

    Args:
      paths: list of string path where the file object should be deleted
    """
    def _delete(path):
      """Recursively delete the file or directory at the provided path.
      """
      if path.endswith('/'):
        path += '*'
      metadata_list = self.match([path])[0]
      gcsio.GcsIO().delete_batch([m.path for m in metadata_list])
    for path in paths:
      _delete(path)

  def delete_directory(self, path):
    """Delete the directory at the particular path.

    Args:
      path: path of the directory that needs to be deleted
    """
    if not path.endswith('/'):
      path += '/'
    GCSFileSystem.delete([path])
