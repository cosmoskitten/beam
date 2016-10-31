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

"""A framework for developing sources for new file types.

To create a source for a new file type a sub-class of ``FileBasedSource`` should
be created. Sub-classes of ``FileBasedSource`` must implement the method
``FileBasedSource.read_records()``. Please read the documentation of that method
for more details.

For an example implementation of ``FileBasedSource`` see ``avroio.AvroSource``.
"""

import random

from multiprocessing.pool import ThreadPool

from apache_beam.internal import pickler
from apache_beam.io import concat_source
from apache_beam.io import fileio
from apache_beam.io import iobase
from apache_beam.io import range_trackers

MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 25


class FileBasedSource(iobase.BoundedSource):
  """A ``BoundedSource`` for reading a file glob of a given type."""

  MAX_NUMBER_OF_FILES_FOR_AN_EXACT_STAT = 100
  FRACTION_OF_FILES_TO_STAT = 0.01

  def __init__(self,
               file_pattern,
               min_bundle_size=0,
               compression_type=fileio.CompressionTypes.AUTO,
               splittable=True):
    """Initializes ``FileBasedSource``.

    Args:
      file_pattern: the file glob to read.
      min_bundle_size: minimum size of bundles that should be generated when
                       performing initial splitting on this source.
      compression_type: compression type to use
      splittable: whether FileBasedSource should try to logically split a single
                  file into data ranges so that different parts of the same file
                  can be read in parallel. If set to False, FileBasedSource will
                  prevent both initial and dynamic splitting of sources for
                  single files. File patterns that represent multiple files may
                  still get split into sources for individual files. Even if set
                  to True by the user, FileBasedSource may choose to not split
                  the file, for example, for compressed files where currently
                  it is not possible to efficiently read a data range without
                  decompressing the whole file.
    Raises:
      TypeError: when compression_type is not valid or if file_pattern is not a
                 string.
      ValueError: when compression and splittable files are specified.
    """
    if not isinstance(file_pattern, basestring):
      raise TypeError(
          '%s: file_pattern must be a string;  got %r instead' %
          (self.__class__.__name__, file_pattern))

    self._pattern = file_pattern
    self._concat_source = None
    self._min_bundle_size = min_bundle_size
    if not fileio.CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError('compression_type must be CompressionType object but '
                      'was %s' % type(compression_type))
    self._compression_type = compression_type
    if compression_type in (fileio.CompressionTypes.UNCOMPRESSED,
                            fileio.CompressionTypes.AUTO):
      self._splittable = splittable
    else:
      # We can't split compressed files efficiently so turn off splitting.
      self._splittable = False

  def _get_concat_source(self):
    if self._concat_source is None:
      single_file_sources = []
      file_names = [f for f in fileio.ChannelFactory.glob(self._pattern)]
      sizes = FileBasedSource._estimate_sizes_in_parallel(file_names)

      for index, file_name in enumerate(file_names):
        if sizes[index] == 0:
          continue  # Ignoring empty file.

        # We determine splittability of this specific file.
        splittable = self.splittable
        if (splittable and
            self._compression_type == fileio.CompressionTypes.AUTO):
          compression_type = fileio.CompressionTypes.detect_compression_type(
              file_name)
          if compression_type != fileio.CompressionTypes.UNCOMPRESSED:
            splittable = False

        single_file_source = _SingleFileSource(
            self, file_name,
            0,
            sizes[index],
            min_bundle_size=self._min_bundle_size,
            splittable=splittable)
        single_file_sources.append(single_file_source)
      self._concat_source = concat_source.ConcatSource(single_file_sources)
    return self._concat_source

  def open_file(self, file_name):
    return fileio.ChannelFactory.open(
        file_name, 'rb', 'application/octet-stream',
        compression_type=self._compression_type)

  @staticmethod
  def _estimate_sizes_in_parallel(file_names):

    if not file_names:
      return []
    elif len(file_names) == 1:
      return [fileio.ChannelFactory.size_in_bytes(file_names[0])]
    else:
      pool = ThreadPool(
          min(MAX_NUM_THREADS_FOR_SIZE_ESTIMATION, len(file_names)))
      try:
        return pool.map(fileio.ChannelFactory.size_in_bytes, file_names)
      finally:
        pool.terminate()

  def split(
      self, desired_bundle_size=None, start_position=None, stop_position=None):
    return self._get_concat_source().split(
        desired_bundle_size=desired_bundle_size,
        start_position=start_position,
        stop_position=stop_position)

  def estimate_size(self):
    file_names = [f for f in fileio.ChannelFactory.glob(self._pattern)]
    if (len(file_names) <=
        FileBasedSource.MAX_NUMBER_OF_FILES_FOR_AN_EXACT_STAT):
      return sum(self._estimate_sizes_in_parallel(file_names))
    else:
      # Estimating size of a random sample.
      sample_size = max(FileBasedSource.MAX_NUMBER_OF_FILES_FOR_AN_EXACT_STAT,
                        int(len(file_names) *
                            FileBasedSource.FRACTION_OF_FILES_TO_STAT))
      sample = [file_names[i] for i in sorted(
          random.sample(xrange(len(file_names)), sample_size))]
      return int(
          sum(self._estimate_sizes_in_parallel(sample)) *
          (float(len(file_names)) / len(sample)))

  def read(self, range_tracker):
    return self._get_concat_source().read(range_tracker)

  def get_range_tracker(self, start_position, stop_position):
    return self._get_concat_source().get_range_tracker(start_position,
                                                       stop_position)

  def default_output_coder(self):
    return self._get_concat_source().default_output_coder()

  def read_records(self, file_name, offset_range_tracker):
    """Returns a generator of records created by reading file 'file_name'.

    Args:
      file_name: a ``string`` that gives the name of the file to be read. Method
                 ``FileBasedSource.open_file()`` must be used to open the file
                 and create a seekable file object.
      offset_range_tracker: a object of type ``OffsetRangeTracker``. This
                            defines the byte range of the file that should be
                            read. See documentation in
                            ``iobase.BoundedSource.read()`` for more information
                            on reading records while complying to the range
                            defined by a given ``RangeTracker``.

    Returns:
      a iterator that gives the records read from the given file.
    """
    raise NotImplementedError

  @property
  def splittable(self):
    return self._splittable


class _SingleFileSource(iobase.BoundedSource):
  """Denotes a source for a specific file type."""

  def __init__(self, file_based_source, file_name, start_offset, stop_offset,
               min_bundle_size=0, splittable=True):
    if not isinstance(start_offset, (int, long)):
      raise TypeError(
          'start_offset must be a number. Received: %r' % start_offset)
    if stop_offset != range_trackers.OffsetRangeTracker.OFFSET_INFINITY:
      if not isinstance(stop_offset, (int, long)):
        raise TypeError(
            'stop_offset must be a number. Received: %r' % stop_offset)
      if start_offset >= stop_offset:
        raise ValueError(
            'start_offset must be smaller than stop_offset. Received %d and %d '
            'for start and stop offsets respectively' %
            (start_offset, stop_offset))

    self._file_name = file_name
    self._is_gcs_file = file_name.startswith('gs://') if file_name else False
    self._start_offset = start_offset
    self._stop_offset = stop_offset
    self._min_bundle_size = min_bundle_size
    self._file_based_source = file_based_source
    self._splittable = splittable

  def split(self, desired_bundle_size, start_offset=None, stop_offset=None):
    if start_offset is None:
      start_offset = self._start_offset
    if stop_offset is None:
      stop_offset = self._stop_offset

    if self._splittable:
      bundle_size = max(desired_bundle_size, self._min_bundle_size)

      bundle_start = start_offset
      while bundle_start < stop_offset:
        bundle_stop = min(bundle_start + bundle_size, stop_offset)
        yield iobase.SourceBundle(
            bundle_stop - bundle_start,
            _SingleFileSource(
                # Copying this so that each sub-source gets a fresh instance.
                pickler.loads(pickler.dumps(self._file_based_source)),
                self._file_name,
                bundle_start,
                bundle_stop,
                min_bundle_size=self._min_bundle_size,
                splittable=self._splittable),
            bundle_start,
            bundle_stop)
        bundle_start = bundle_stop
    else:
      # Returning a single sub-source with end offset set to OFFSET_INFINITY (so
      # that all data of the source gets read) since this source is
      # unsplittable. Choosing size of the file as end offset will be wrong for
      # certain unsplittable source, e.g., compressed sources.
      yield iobase.SourceBundle(
          stop_offset - start_offset,
          _SingleFileSource(
              self._file_based_source,
              self._file_name,
              start_offset,
              range_trackers.OffsetRangeTracker.OFFSET_INFINITY,
              min_bundle_size=self._min_bundle_size,
              splittable=self._splittable
          ),
          start_offset,
          range_trackers.OffsetRangeTracker.OFFSET_INFINITY
      )

  def estimate_size(self):
    return self._stop_offset - self._start_offset

  def get_range_tracker(self, start_position, stop_position):
    if start_position is None:
      start_position = self._start_offset
    if stop_position is None:
      # If file is unsplittable we choose OFFSET_INFINITY as the default end
      # offset so that all data of the source gets read. Choosing size of the
      # file as end offset will be wrong for certain unsplittable source, for
      # e.g., compressed sources.
      stop_position = (
          self._stop_offset if self._splittable
          else range_trackers.OffsetRangeTracker.OFFSET_INFINITY)

    range_tracker = range_trackers.OffsetRangeTracker(
        start_position, stop_position)
    if not self._splittable:
      range_tracker = range_trackers.UnsplittableRangeTracker(range_tracker)

    return range_tracker

  def read(self, range_tracker):
    return self._file_based_source.read_records(self._file_name, range_tracker)
