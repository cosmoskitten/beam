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
"""``PTransforms`` for reading from and writing to Parquet files.

Provides two read ``PTransform``s, ``ReadFromParquet`` and
``ReadAllFromParquet``, that produces a ``PCollection`` of records.
Each record of this ``PCollection`` will contain a single record read from
a Parquet file. Records that are of simple types will be mapped into
corresponding Python types. The actual parquet file operations are managed by
pyarrow. Since pyarrow does not expose its low level metadata APIs, splitting
feature is disabled for the parquet source.

Additionally, this module provides a write ``PTransform`` ``WriteToParquet``
that can be used to write a given ``PCollection`` of Python objects to a
Parquet file.
"""

from functools import partial

import pyarrow as pa
from pyarrow.parquet import ParquetFile
from pyarrow.parquet import ParquetWriter

from apache_beam.io import filebasedsink
from apache_beam.io import filebasedsource
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.iobase import Read
from apache_beam.io.iobase import Write
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.transforms import PTransform

__all__ = ['ReadFromParquet', 'ReadAllFromParquet', 'WriteToParquet']


class ReadFromParquet(PTransform):
  """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading
     Parquet files."""

  def __init__(self, file_pattern=None, min_bundle_size=0, validate=True):
    """Initialize :class:`ReadFromParquet`.
    """
    super(ReadFromParquet, self).__init__()
    self._source = _create_parquet_source(
        file_pattern,
        min_bundle_size,
        validate=validate
    )

  def expand(self, pvalue):
    return pvalue.pipeline | Read(self._source)

  def display_data(self):
    return {'source_dd': self._source}


class ReadAllFromParquet(PTransform):
  """A ``PTransform`` for reading ``PCollection`` of Parquet files.

   Uses source '_ParquetSource' to read a ``PCollection`` of Parquet files or
   file patterns and produce a ``PCollection`` of Parquet records.
  """

  DEFAULT_DESIRED_BUNDLE_SIZE = 64 * 1024 * 1024  # 64MB

  def __init__(self, min_bundle_size=0,
               desired_bundle_size=DEFAULT_DESIRED_BUNDLE_SIZE,
               label='ReadAllFiles'):
    """Initializes ``ReadAllFromParquet``.

    Args:
      min_bundle_size: the minimum size in bytes, to be considered when
                       splitting the input into bundles.
      desired_bundle_size: the desired size in bytes, to be considered when
                       splitting the input into bundles.
    """
    super(ReadAllFromParquet, self).__init__()
    source_from_file = partial(
        _create_parquet_source,
        min_bundle_size=min_bundle_size
    )
    self._read_all_files = filebasedsource.ReadAllFiles(
        False, CompressionTypes.AUTO, desired_bundle_size, min_bundle_size,
        source_from_file)

    self.label = label

  def expand(self, pvalue):
    return pvalue | self.label >> self._read_all_files


def _create_parquet_source(file_pattern=None,
                           min_bundle_size=None,
                           validate=False):
  return \
    _ParquetSource(
        file_pattern=file_pattern,
        min_bundle_size=min_bundle_size,
        validate=validate,
        splittable=False
    )


class _ParquetSource(filebasedsource.FileBasedSource):
  """A source for reading Parquet files.
  """

  def read_records(self, file_name, range_tracker):
    if (range_tracker.start_position() != 0 or
        range_tracker.stop_position() != OffsetRangeTracker.OFFSET_INFINITY):
      raise ValueError("parquet source is not splittable.")
    with self.open_file(file_name) as f:
      pf = ParquetFile(f)
      # read the row groups one by one to reduce the memory usage.
      for i in range(pf.num_row_groups):
        table = pf.read_row_group(i)
        num_rows = table.num_rows
        data_items = table.to_pydict().items()
        for n in range(num_rows):
          data = {}
          for label, d in data_items:
            data[label] = d[n]
          yield data


class WriteToParquet(PTransform):
  """A ``PTransform`` for writing parquet files."""

  def __init__(self,
               file_path_prefix,
               schema,
               codec='none',
               row_group_size=1000,
               file_name_suffix='',
               num_shards=0,
               shard_name_template=None,
               mime_type='application/x-parquet'):
    """Initialize a WriteToParquet transform.

    Args:
      file_path_prefix: The file path to write to. The files written will begin
        with this prefix, followed by a shard identifier (see num_shards), and
        end in a common extension, if given by file_name_suffix. In most cases,
        only this argument is specified and num_shards, shard_name_template, and
        file_name_suffix use default values.
      schema: The schema to use, as type of pyarrow.Schema
      codec: The codec to use for block-level compression. Any string supported
        by the pyarrow specification is accepted.
      row_group_size: The number of records in each row group.
      file_name_suffix: Suffix for the files written.
      num_shards: The number of files (shards) used for output. If not set, the
        service will decide on the optimal number of shards.
        Constraining the number of shards is likely to reduce
        the performance of a pipeline.  Setting this value is not recommended
        unless you require a specific number of output files.
      shard_name_template: A template string containing placeholders for
        the shard number and shard count. When constructing a filename for a
        particular shard number, the upper-case letters 'S' and 'N' are
        replaced with the 0-padded shard number and shard count respectively.
        This argument can be '' in which case it behaves as if num_shards was
        set to 1 and only one file will be generated. The default pattern used
        is '-SSSSS-of-NNNNN' if None is passed as the shard_name_template.
      mime_type: The MIME type to use for the produced files, if the filesystem
        supports specifying MIME types.

    Returns:
      A WriteToParquet transform usable for writing.
    """
    super(WriteToParquet, self).__init__()
    self._sink = \
      _create_parquet_sink(
          file_path_prefix,
          schema,
          codec,
          row_group_size,
          file_name_suffix,
          num_shards,
          shard_name_template,
          mime_type
      )

  def expand(self, pcoll):
    return pcoll | Write(self._sink)

  def display_data(self):
    return {'sink_dd': self._sink}


def _create_parquet_sink(file_path_prefix,
                         schema,
                         codec,
                         row_group_size,
                         file_name_suffix,
                         num_shards,
                         shard_name_template,
                         mime_type):
  return \
    _ParquetSink(
        file_path_prefix,
        schema,
        codec,
        row_group_size,
        file_name_suffix,
        num_shards,
        shard_name_template,
        mime_type
    )


class _ParquetSink(filebasedsink.FileBasedSink):
  """A sink for parquet files."""

  def __init__(self,
               file_path_prefix,
               schema,
               codec,
               row_group_size,
               file_name_suffix,
               num_shards,
               shard_name_template,
               mime_type):
    super(_ParquetSink, self).__init__(
        file_path_prefix,
        file_name_suffix=file_name_suffix,
        num_shards=num_shards,
        shard_name_template=shard_name_template,
        coder=None,
        mime_type=mime_type,
        # Compression happens at the block level using the supplied codec, and
        # not at the file level.
        compression_type=CompressionTypes.UNCOMPRESSED)
    self._schema = schema
    self._codec = codec
    self._row_group_size = row_group_size
    self._buffer = [[] for _ in range(len(schema.names))]
    self._file_handle = None

  def open(self, temp_path):
    self._file_handle = super(_ParquetSink, self).open(temp_path)
    return ParquetWriter(
        self._file_handle, self._schema, compression=self._codec)

  def write_record(self, writer, value):
    if len(self._buffer[0]) >= self._row_group_size:
      self._write_buffer(writer)

    # reorder the data in columnar format.
    for i, n in enumerate(self._schema.names):
      self._buffer[i].append(value[n])

  def close(self, writer):
    if len(self._buffer[0]) > 0:
      self._write_buffer(writer)

    writer.close()
    if self._file_handle:
      self._file_handle.close()
      self._file_handle = None

  def display_data(self):
    res = super(_ParquetSink, self).display_data()
    res['codec'] = str(self._codec)
    res['schema'] = str(self._schema)
    return res

  def _write_buffer(self, writer):
    for x, y in enumerate(self._buffer):
      self._buffer[x] = pa.array(y)
    table = pa.Table.from_arrays(self._buffer, self._schema.names)
    writer.write_table(table)
    self._buffer = [[] for _ in range(len(self._schema.names))]
