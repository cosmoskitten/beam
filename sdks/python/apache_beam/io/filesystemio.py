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
"""Utilities for ``FileSystem`` implementations."""

import abc
import cStringIO
import logging
import multiprocessing
import os
import traceback

__all__ = ['Downloader', 'Uploader', 'BufferedReader', 'BufferedWriter']


class Downloader(object):
  """A download interface for a single file.

  Ranges to download are requested in get_range. Data is streamed to a
  user-provided download_stream.
  """

  __metaclass__ = abc.ABCMeta

  @abc.abstractproperty
  def size(self):
    """Size of the file to download"""

  @abc.abstractmethod
  def start(self, download_stream, buffer_size):
    """Initialize downloader.

    Args:
      download_stream: (cStringIO.StringIO) A buffer where downloaded data is
        streamed to.
      buffer_size: Maximum range size for get_range calls.
    """

  @abc.abstractmethod
  def get_range(self, start, end):
    """Retrieve a given byte range from this download, inclusive.

    Range must be in this form:
      0 <= start <= end: Fetch the bytes from start to end.

    Args:
      start: (int) Where to start fetching bytes. (See above.)
      end: (int) Where to stop fetching bytes. (See above.)

    Returns:
      None. Streams bytes to download_stream provided in call to start.
    """


class Uploader(object):
  """An upload interface for a single file.

  Data to upload is sent via ``multiprocessing.Pipe`` connection objects.
  """

  __metaclass__ = abc.ABCMeta

  @abc.abstractproperty
  def last_error(self):
    """Last error encountered for this instance."""

  @abc.abstractmethod
  def start(self, upload_conn):
    """Initialize uploader to upload data sent to upload_conn.

    Closes upload_conn on error. Use last_error to get actual error.

    Args:
      upload_conn: (multiprocessing.Connection) Where data to upload will be
        sent to.
    """

  @abc.abstractmethod
  def finish(self):
    """Upload any remaining data and close the file."""


# TODO: Consider using cStringIO instead of buffers and data_lists when reading.
class BufferedReader(object):
  """A class for reading files from stateless services.

  Provides a Python File object-like interface.
  """

  def __init__(self,
               downloader,
               path,
               buffer_size,
               mode='r'):
    """Initialize reader and downloader.

    Args:
      downloader: (Downloader) Filesystem dependent implementation.
      path: (string) Path of file to download. Used for logging only.
      buffer_size: (int) How many bytes to read at a time.
      mode: (string) Python file mode to return (for Python File compatibility).
    """
    self.downloader = downloader
    self.path = path
    self.mode = mode
    self.buffer_size = buffer_size

    # Initialize read buffer state.
    self.download_stream = cStringIO.StringIO()
    self.downloader.start(self.download_stream, self.buffer_size)
    self.position = 0
    self.buffer = ''
    self.buffer_start_position = 0
    self.closed = False

  def __iter__(self):
    return self

  def __next__(self):
    """Read one line delimited by '\\n' from the file.
    """
    return next(self)

  def next(self):
    """Read one line delimited by '\\n' from the file.
    """
    line = self.readline()
    if not line:
      raise StopIteration
    return line

  def read(self, size=-1):
    """Read data from a file.

    Args:
      size: Number of bytes to read. Actual number of bytes read is always
            equal to size unless EOF is reached. If size is negative or
            unspecified, read the entire file.

    Returns:
      data read as str.

    Raises:
      IOError: When this buffer is closed.
    """
    return self._read_inner(size=size, readline=False)

  def readline(self, size=-1):
    """Read one line delimited by '\\n' from the file.

    Mimics behavior of the readline() method on standard file objects.

    A trailing newline character is kept in the string. It may be absent when a
    file ends with an incomplete line. If the size argument is non-negative,
    it specifies the maximum string size (counting the newline) to return.
    A negative size is the same as unspecified. Empty string is returned
    only when EOF is encountered immediately.

    Args:
      size: Maximum number of bytes to read. If not specified, readline stops
        only on '\\n' or EOF.

    Returns:
      The data read as a string.

    Raises:
      IOError: When this buffer is closed.
    """
    return self._read_inner(size=size, readline=True)

  def _read_inner(self, size=-1, readline=False):
    """Shared implementation of read() and readline()."""
    self._check_open()
    if not self._remaining():
      return ''

    # Prepare to read.
    data_list = []
    if size is None:
      size = -1
    to_read = min(size, self._remaining())
    if to_read < 0:
      to_read = self._remaining()
    break_after = False

    while to_read > 0:
      # If we have exhausted the buffer, get the next segment.
      # TODO(ccy): We should consider prefetching the next block in another
      # thread.
      self._fetch_next_if_buffer_exhausted()

      # Determine number of bytes to read from buffer.
      buffer_bytes_read = self.position - self.buffer_start_position
      bytes_to_read_from_buffer = min(
          len(self.buffer) - buffer_bytes_read, to_read)

      # If readline is set, we only want to read up to and including the next
      # newline character.
      if readline:
        next_newline_position = self.buffer.find('\n', buffer_bytes_read,
                                                 len(self.buffer))
        if next_newline_position != -1:
          bytes_to_read_from_buffer = (
              1 + next_newline_position - buffer_bytes_read)
          break_after = True

      # Read bytes.
      data_list.append(self.buffer[buffer_bytes_read:buffer_bytes_read +
                                   bytes_to_read_from_buffer])
      self.position += bytes_to_read_from_buffer
      to_read -= bytes_to_read_from_buffer

      if break_after:
        break

    return ''.join(data_list)

  def _fetch_next_if_buffer_exhausted(self):
    if not self.buffer or (
        self.buffer_start_position + len(self.buffer) <= self.position):
      bytes_to_request = min(self._remaining(), self.buffer_size)
      self.buffer_start_position = self.position
      try:
        result = self._get_segment(self.position, bytes_to_request)
      except Exception as e:  # pylint: disable=broad-except
        tb = traceback.format_exc()
        logging.error(
            ('Exception while fetching %d bytes from position %d of %s: '
             '%s\n%s'),
            bytes_to_request, self.position, self.path, e, tb)
        raise

      self.buffer = result
      return

  def _remaining(self):
    return self.downloader.size - self.position

  def close(self):
    """Close the current file."""
    self.closed = True
    self.download_stream = None
    self.downloader = None
    self.buffer = None

  def _get_segment(self, start, size):
    """Get the given segment of the current file."""
    if size == 0:
      return ''
    end = start + size - 1
    self.downloader.get_range(start, end)
    value = self.download_stream.getvalue()
    # Clear the cStringIO object after we've read its contents.
    self.download_stream.truncate(0)
    assert len(value) == size
    return value

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self.close()

  def seek(self, offset, whence=os.SEEK_SET):
    """Set the file's current offset.

    Note if the new offset is out of bound, it is adjusted to either 0 or EOF.

    Args:
      offset: seek offset as number.
      whence: seek mode. Supported modes are os.SEEK_SET (absolute seek),
        os.SEEK_CUR (seek relative to the current position), and os.SEEK_END
        (seek relative to the end, offset should be negative).

    Raises:
      IOError: When this buffer is closed.
      ValueError: When whence is invalid.
    """
    self._check_open()

    self.buffer = ''
    self.buffer_start_position = -1

    if whence == os.SEEK_SET:
      self.position = offset
    elif whence == os.SEEK_CUR:
      self.position += offset
    elif whence == os.SEEK_END:
      self.position = self.downloader.size + offset
    else:
      raise ValueError('Whence mode %r is invalid.' % whence)

    self.position = min(self.position, self.downloader.size)
    self.position = max(self.position, 0)

  def tell(self):
    """Tell the file's current offset.

    Returns:
      current offset in reading this file.

    Raises:
      IOError: When this buffer is closed.
    """
    self._check_open()
    return self.position

  def _check_open(self):
    if self.closed:
      raise IOError('Buffer is closed.')

  def seekable(self):
    return True

  def readable(self):
    return True

  def writable(self):
    return False


# TODO: Consider using cStringIO instead of buffers and data_lists when reading
# and writing.
class BufferedWriter(object):
  """A class for writing files from stateless services.

  Provides a Python File object-like interface.
  """

  class PipeStream(object):
    """A class that presents a pipe connection as a readable stream."""

    def __init__(self, recv_pipe):
      self.conn = recv_pipe
      self.closed = False
      self.position = 0
      self.remaining = ''

    def read(self, size):
      """Read data from the wrapped pipe connection.

      Args:
        size: Number of bytes to read. Actual number of bytes read is always
              equal to size unless EOF is reached.

      Returns:
        data read as str.
      """
      data_list = []
      bytes_read = 0
      while bytes_read < size:
        bytes_from_remaining = min(size - bytes_read, len(self.remaining))
        data_list.append(self.remaining[0:bytes_from_remaining])
        self.remaining = self.remaining[bytes_from_remaining:]
        self.position += bytes_from_remaining
        bytes_read += bytes_from_remaining
        if not self.remaining:
          try:
            self.remaining = self.conn.recv_bytes()
          except EOFError:
            break
      return ''.join(data_list)

    def tell(self):
      """Tell the file's current offset.

      Returns:
        current offset in reading this file.

      Raises:
        IOError: When this stream is closed.
      """
      self._check_open()
      return self.position

    def seek(self, offset, whence=os.SEEK_SET):
      # The apitools.base.py.transfer.Upload class insists on seeking to the end
      # of a stream to do a check before completing an upload, so we must have
      # this no-op method here in that case.
      if whence == os.SEEK_END and offset == 0:
        return
      elif whence == os.SEEK_SET and offset == self.position:
        return
      raise NotImplementedError

    def _check_open(self):
      if self.closed:
        raise IOError('Stream is closed.')

  def __init__(self,
               uploader,
               path,
               mode='w',
               mime_type='application/octet-stream'):
    self.uploader = uploader
    self.path = path
    self.mode = mode
    self.closed = False
    self.position = 0

    # A small buffer to avoid CPU-heavy per-write pipe calls.
    self.write_buffer = bytearray()
    self.write_buffer_size = 128 * 1024

    # Set up communication with uploader.
    parent_conn, child_conn = multiprocessing.Pipe()
    self.child_conn = child_conn
    self.conn = parent_conn

    self.uploader.start(child_conn)

  def write(self, data):
    """Write data to a file.

    Args:
      data: data to write as str.

    Raises:
      IOError: When this buffer is closed.
    """
    self._check_open()
    if not data:
      return
    self.write_buffer.extend(data)
    if len(self.write_buffer) > self.write_buffer_size:
      self._flush_write_buffer()
    self.position += len(data)

  def flush(self):
    """Flushes any internal buffer to the underlying file."""
    self._check_open()
    self._flush_write_buffer()

  def tell(self):
    """Return the total number of bytes passed to write() so far."""
    return self.position

  def close(self):
    """Close the current file."""
    if self.closed:
      logging.warn('Channel for %s is not open.', self.path)
      return

    self._flush_write_buffer()
    self.closed = True
    self.conn.close()
    self.uploader.finish()

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self.close()

  def _check_open(self):
    if self.closed:
      raise IOError('Buffer is closed.')

  def seekable(self):
    return False

  def readable(self):
    return False

  def writable(self):
    return True

  def _flush_write_buffer(self):
    try:
      self.conn.send_bytes(buffer(self.write_buffer))
      self.write_buffer = bytearray()
    except IOError:
      # TODO(udim): Call self.uploader.finish() instead (once
      # GcsUploader.finish() calls join() with timeout), and remove last_error
      # property. Rely on finish() to raise any internal errors encountered.
      if self.uploader.last_error:
        raise self.uploader.last_error  # pylint: disable=raising-bad-type
      else:
        raise
