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

  @staticmethod
  def mkdirs(path):
    """Recursively create directories for the provided path.

    Args:
      path: string path of the directory structure that should be created

    Raises:
      IOError if leaf directory already exists.
    """
    pass

  @staticmethod
  def match(pattern, limit=None):
    """Find all matching paths to the pattern provided.

    Args:
      pattern: string for the file path pattern to match against
      limit: Maximum number of responses that need to be fetched

    Returns: List of FileMetadata objects that match the provided pattern.
    """
    file_sizes = gcsio.GcsIO().size_of_files_in_glob(pattern)
    return [FileMetadata(path, size) for path, size in file_sizes.iteritems()]

  @staticmethod
  def _path_open(path, mode, mime_type='application/octet-stream',
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

  @staticmethod
  def create(path, mime_type='application/octet-stream',
             compression_type=CompressionTypes.AUTO):
    """Returns a write channel for the given file path.

    Args:
      path: string path of the file object to be written to the system
      mime_type: MIME type to specify the type on content in the file object
      compression_type: Type of compression to be used for this object
    """
    return GCSFileSystem._path_open(path, 'wb', mime_type, compression_type)

  @staticmethod
  def open(path, mime_type='application/octet-stream',
           compression_type=CompressionTypes.AUTO):
    """Returns a read channel for the given file path.

    Args:
      path: string path of the file object to be written to the system
      mime_type: MIME type to specify the type on content in the file object
      compression_type: Type of compression to be used for this object
    """
    return GCSFileSystem._path_open(path, 'rb', mime_type, compression_type)

  @staticmethod
  def copy(source, destination):
    """Recursively copy the file tree from the source to the destination

    Args:
      source: Source file object that needs to be copied
      destination: Destination of the new file object
    """
    if not destination.startswith('gs://'):
      raise ValueError('Destination %r must be GCS path.', destination)
    assert source.endswith('/'), source
    assert destination.endswith('/'), destination
    gcsio.GcsIO().copytree(source, destination)

  @staticmethod
  def rename(sources, destinations):
    """Rename the files at the source to the destination paths.
    Sources and destinations lists should be of the same size.


    Args:
      sources: List of file paths that need to be moved
      destinations: List of respective destinations for the file objects
    """
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

  @staticmethod
  def exists(path):
    """Check if the provided path exists on the FileSystem.

    Args:
      path: string path that needs to be checked.
    """
    return gcsio.GcsIO().exists(path)

  @staticmethod
  def delete(path):
    """Recursively delete the file or directory at the provided path.

    Args:
      path: string path where the file object should be deleted
    """
    if path.endswith('/'):
      path += '*'
    for file_metadata in GCSFileSystem.match(path):
      gcsio.GcsIO().delete(file_metadata.path)

  @staticmethod
  def delete_directory(path):
    """Delete the directory at the particular path.

    Args:
      path: path of the directory that needs to be deleted
    """
    if not path.endswith('/'):
      path += '/'
    GCSFileSystem.delete(path)
