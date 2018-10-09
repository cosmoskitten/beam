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
"""``PTransforms`` for reading from and writing to Avro files.

Provides two read ``PTransform``s, ``ReadFromAvro`` and ``ReadAllFromAvro``,
that produces a ``PCollection`` of records.
Each record of this ``PCollection`` will contain a single record read from
an Avro file. Records that are of simple types will be mapped into
corresponding Python types. Records that are of Avro type 'RECORD' will be
mapped to Python dictionaries that comply with the schema contained in the
Avro file that contains those records. In this case, keys of each dictionary
will contain the corresponding field names and will be of type ``string``
while the values of the dictionary will be of the type defined in the
corresponding Avro schema.

For example, if schema of the Avro file is the following.
{"namespace": "example.avro","type": "record","name": "User","fields":
[{"name": "name", "type": "string"},
{"name": "favorite_number",  "type": ["int", "null"]},
{"name": "favorite_color", "type": ["string", "null"]}]}

Then records generated by read transforms will be dictionaries of the
following form.
{u'name': u'Alyssa', u'favorite_number': 256, u'favorite_color': None}).

Additionally, this module provides a write ``PTransform`` ``WriteToAvro``
that can be used to write a given ``PCollection`` of Python objects to an
Avro file.
"""
from __future__ import absolute_import

import io
import os
import zlib
from builtins import object
from functools import partial

import avro
from avro import io as avroio
from avro import datafile
from fastavro.read import block_reader
from fastavro.write import Writer
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from avro.schema import Parse # avro-python3 library for python3
except ImportError:
  from avro.schema import parse as Parse # avro library for python2
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

import apache_beam as beam
from apache_beam.io import filebasedsink
from apache_beam.io import filebasedsource
from apache_beam.io import iobase
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.iobase import Read
from apache_beam.transforms import PTransform

__all__ = ['ReadFromAvro', 'ReadAllFromAvro', 'WriteToAvro']


class ReadFromAvro(PTransform):
  """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading avro
  files."""

  def __init__(self, file_pattern=None, min_bundle_size=0, validate=True,
               use_fastavro=False):
    """Initializes :class:`ReadFromAvro`.

    Uses source :class:`~apache_beam.io._AvroSource` to read a set of Avro
    files defined by a given file pattern.

    If ``/mypath/myavrofiles*`` is a file-pattern that points to a set of Avro
    files, a :class:`~apache_beam.pvalue.PCollection` for the records in
    these Avro files can be created in the following manner.

    .. testcode::

      with beam.Pipeline() as p:
        records = p | 'Read' >> beam.io.ReadFromAvro('/mypath/myavrofiles*')

    .. NOTE: We're not actually interested in this error; but if we get here,
       it means that the way of calling this transform hasn't changed.

    .. testoutput::
      :hide:

      Traceback (most recent call last):
       ...
      IOError: No files found based on the file pattern

    Each record of this :class:`~apache_beam.pvalue.PCollection` will contain
    a single record read from a source. Records that are of simple types will be
    mapped into corresponding Python types. Records that are of Avro type
    ``RECORD`` will be mapped to Python dictionaries that comply with the schema
    contained in the Avro file that contains those records. In this case, keys
    of each dictionary will contain the corresponding field names and will be of
    type :class:`str` while the values of the dictionary will be of the type
    defined in the corresponding Avro schema.

    For example, if schema of the Avro file is the following. ::

      {
        "namespace": "example.avro",
        "type": "record",
        "name": "User",
        "fields": [

          {"name": "name",
           "type": "string"},

          {"name": "favorite_number",
           "type": ["int", "null"]},

          {"name": "favorite_color",
           "type": ["string", "null"]}

        ]
      }

    Then records generated by :class:`~apache_beam.io._AvroSource` will be
    dictionaries of the following form. ::

      {u'name': u'Alyssa', u'favorite_number': 256, u'favorite_color': None}).

    Args:
      file_pattern (str): the file glob to read
      min_bundle_size (int): the minimum size in bytes, to be considered when
        splitting the input into bundles.
      validate (bool): flag to verify that the files exist during the pipeline
        creation time.
      use_fastavro (bool); when set, use the `fastavro` library for IO, which
        is significantly faster, and will likely become the default
    """
    super(ReadFromAvro, self).__init__()
    self._source = _create_avro_source(
        file_pattern,
        min_bundle_size,
        validate=validate,
        use_fastavro=use_fastavro
    )

  def expand(self, pvalue):
    return pvalue.pipeline | Read(self._source)

  def display_data(self):
    return {'source_dd': self._source}


class ReadAllFromAvro(PTransform):
  """A ``PTransform`` for reading ``PCollection`` of Avro files.

   Uses source '_AvroSource' to read a ``PCollection`` of Avro files or
   file patterns and produce a ``PCollection`` of Avro records.
  """

  DEFAULT_DESIRED_BUNDLE_SIZE = 64 * 1024 * 1024  # 64MB

  def __init__(self, min_bundle_size=0,
               desired_bundle_size=DEFAULT_DESIRED_BUNDLE_SIZE,
               use_fastavro=False,
               label='ReadAllFiles'):
    """Initializes ``ReadAllFromAvro``.

    Args:
      min_bundle_size: the minimum size in bytes, to be considered when
                       splitting the input into bundles.
      desired_bundle_size: the desired size in bytes, to be considered when
                       splitting the input into bundles.
    """
    source_from_file = partial(
        _create_avro_source,
        min_bundle_size=min_bundle_size,
        use_fastavro=use_fastavro
    )
    self._read_all_files = filebasedsource.ReadAllFiles(
        True, CompressionTypes.AUTO, desired_bundle_size, min_bundle_size,
        source_from_file)

    self.label = label

  def expand(self, pvalue):
    return pvalue | self.label >> self._read_all_files


class _AvroUtils(object):

  @staticmethod
  def read_meta_data_from_file(f):
    """Reads metadata from a given Avro file.

    Args:
      f: Avro file to read.
    Returns:
      a tuple containing the codec, schema, and the sync marker of the Avro
      file.

    Raises:
      ValueError: if the file does not start with the byte sequence defined in
                  the specification.
    """
    if f.tell() > 0:
      f.seek(0)
    decoder = avroio.BinaryDecoder(f)
    header = avroio.DatumReader().read_data(datafile.META_SCHEMA,
                                            datafile.META_SCHEMA, decoder)
    if header.get('magic') != datafile.MAGIC:
      raise ValueError('Not an Avro file. File header should start with %s but'
                       'started with %s instead.'
                       % (datafile.MAGIC, header.get('magic')))

    meta = header['meta']

    if datafile.CODEC_KEY in meta:
      codec = meta[datafile.CODEC_KEY]
    else:
      codec = 'null'

    schema_string = meta[datafile.SCHEMA_KEY]
    sync_marker = header['sync']

    return codec, schema_string, sync_marker

  @staticmethod
  def read_block_from_file(f, codec, schema, expected_sync_marker):
    """Reads a block from a given Avro file.

    Args:
      f: Avro file to read.
      codec: The codec to use for block-level decompression.
        Supported codecs: 'null', 'deflate', 'snappy'
      schema: Avro Schema definition represented as JSON string.
      expected_sync_marker: Avro synchronization marker. If the block's sync
        marker does not match with this parameter then ValueError is thrown.
    Returns:
      A single _AvroBlock.

    Raises:
      ValueError: If the block cannot be read properly because the file doesn't
        match the specification.
    """
    offset = f.tell()
    decoder = avroio.BinaryDecoder(f)
    num_records = decoder.read_long()
    block_size = decoder.read_long()
    block_bytes = decoder.read(block_size)
    sync_marker = decoder.read(len(expected_sync_marker))
    if sync_marker != expected_sync_marker:
      raise ValueError('Unexpected sync marker (actual "%s" vs expected "%s"). '
                       'Maybe the underlying avro file is corrupted?'
                       % (sync_marker, expected_sync_marker))
    size = f.tell() - offset
    return _AvroBlock(block_bytes, num_records, codec, schema, offset, size)

  @staticmethod
  def advance_file_past_next_sync_marker(f, sync_marker):
    buf_size = 10000

    data = f.read(buf_size)
    while data:
      pos = data.find(sync_marker)
      if pos >= 0:
        # Adjusting the current position to the ending position of the sync
        # marker.
        backtrack = len(data) - pos - len(sync_marker)
        f.seek(-1 * backtrack, os.SEEK_CUR)
        return True
      else:
        if f.tell() >= len(sync_marker):
          # Backtracking in case we partially read the sync marker during the
          # previous read. We only have to backtrack if there are at least
          # len(sync_marker) bytes before current position. We only have to
          # backtrack (len(sync_marker) - 1) bytes.
          f.seek(-1 * (len(sync_marker) - 1), os.SEEK_CUR)
        data = f.read(buf_size)


def _create_avro_source(file_pattern=None,
                        min_bundle_size=None,
                        validate=False,
                        use_fastavro=False):
  return \
      _FastAvroSource(
          file_pattern=file_pattern,
          min_bundle_size=min_bundle_size,
          validate=validate
      ) \
      if use_fastavro \
      else \
      _AvroSource(
          file_pattern=file_pattern,
          min_bundle_size=min_bundle_size,
          validate=validate
      )


class _AvroBlock(object):
  """Represents a block of an Avro file."""

  def __init__(self, block_bytes, num_records, codec, schema_string,
               offset, size):
    # Decompress data early on (if needed) and thus decrease the number of
    # parallel copies of the data in memory at any given time during block
    # iteration.
    self._decompressed_block_bytes = self._decompress_bytes(block_bytes, codec)
    self._num_records = num_records
    self._schema = Parse(schema_string)
    self._offset = offset
    self._size = size

  def size(self):
    return self._size

  def offset(self):
    return self._offset

  @staticmethod
  def _decompress_bytes(data, codec):
    if codec == 'null':
      return data
    elif codec == 'deflate':
      # zlib.MAX_WBITS is the window size. '-' sign indicates that this is
      # raw data (without headers). See zlib and Avro documentations for more
      # details.
      return zlib.decompress(data, -zlib.MAX_WBITS)
    elif codec == 'snappy':
      # Snappy is an optional avro codec.
      # See Snappy and Avro documentation for more details.
      try:
        import snappy
      except ImportError:
        raise ValueError('Snappy does not seem to be installed.')

      # Compressed data includes a 4-byte CRC32 checksum which we verify.
      # We take care to avoid extra copies of data while slicing large objects
      # by use of a memoryview.
      result = snappy.decompress(memoryview(data)[:-4])
      avroio.BinaryDecoder(io.BytesIO(data[-4:])).check_crc32(result)
      return result
    else:
      raise ValueError('Unknown codec: %r' % codec)

  def num_records(self):
    return self._num_records

  def records(self):
    decoder = avroio.BinaryDecoder(
        io.BytesIO(self._decompressed_block_bytes))
    reader = avroio.DatumReader(
        writers_schema=self._schema, readers_schema=self._schema)

    current_record = 0
    while current_record < self._num_records:
      yield reader.read(decoder)
      current_record += 1


class _AvroSource(filebasedsource.FileBasedSource):
  """A source for reading Avro files.

  ``_AvroSource`` is implemented using the file-based source framework available
  in module 'filebasedsource'. Hence please refer to module 'filebasedsource'
  to fully understand how this source implements operations common to all
  file-based sources such as file-pattern expansion and splitting into bundles
  for parallel processing.
  """

  def read_records(self, file_name, range_tracker):
    next_block_start = -1

    def split_points_unclaimed(stop_position):
      if next_block_start >= stop_position:
        # Next block starts at or after the suggested stop position. Hence
        # there will not be split points to be claimed for the range ending at
        # suggested stop position.
        return 0

      return iobase.RangeTracker.SPLIT_POINTS_UNKNOWN

    range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)

    start_offset = range_tracker.start_position()
    if start_offset is None:
      start_offset = 0

    with self.open_file(file_name) as f:
      codec, schema_string, sync_marker = \
        _AvroUtils.read_meta_data_from_file(f)

      # We have to start at current position if previous bundle ended at the
      # end of a sync marker.
      start_offset = max(0, start_offset - len(sync_marker))
      f.seek(start_offset)
      _AvroUtils.advance_file_past_next_sync_marker(f, sync_marker)

      next_block_start = f.tell()

      while range_tracker.try_claim(next_block_start):
        block = _AvroUtils.read_block_from_file(f, codec, schema_string,
                                                sync_marker)
        next_block_start = block.offset() + block.size()
        for record in block.records():
          yield record


class _FastAvroSource(filebasedsource.FileBasedSource):
  """A source for reading Avro files using the `fastavro` library.

  ``_FastAvroSource`` is implemented using the file-based source framework
  available in module 'filebasedsource'. Hence please refer to module
  'filebasedsource' to fully understand how this source implements operations
  common to all file-based sources such as file-pattern expansion and splitting
  into bundles for parallel processing.

  TODO: remove ``_AvroSource`` in favor of using ``_FastAvroSource``
  everywhere once it has been more widely tested
  """

  def read_records(self, file_name, range_tracker):
    next_block_start = -1

    def split_points_unclaimed(stop_position):
      if next_block_start >= stop_position:
        # Next block starts at or after the suggested stop position. Hence
        # there will not be split points to be claimed for the range ending at
        # suggested stop position.
        return 0

      return iobase.RangeTracker.SPLIT_POINTS_UNKNOWN

    range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)

    start_offset = range_tracker.start_position()
    if start_offset is None:
      start_offset = 0

    with self.open_file(file_name) as f:
      blocks = block_reader(f)
      sync_marker = blocks._header['sync']

      # We have to start at current position if previous bundle ended at the
      # end of a sync marker.
      start_offset = max(0, start_offset - len(sync_marker))
      f.seek(start_offset)
      _AvroUtils.advance_file_past_next_sync_marker(f, sync_marker)

      next_block_start = f.tell()

      while range_tracker.try_claim(next_block_start):
        block = next(blocks)
        next_block_start = block.offset + block.size
        for record in block:
          yield record


class WriteToAvro(beam.transforms.PTransform):
  """A ``PTransform`` for writing avro files."""

  def __init__(self,
               file_path_prefix,
               schema,
               codec='deflate',
               file_name_suffix='',
               num_shards=0,
               shard_name_template=None,
               mime_type='application/x-avro',
               use_fastavro=False):
    """Initialize a WriteToAvro transform.

    Args:
      file_path_prefix: The file path to write to. The files written will begin
        with this prefix, followed by a shard identifier (see num_shards), and
        end in a common extension, if given by file_name_suffix. In most cases,
        only this argument is specified and num_shards, shard_name_template, and
        file_name_suffix use default values.
      schema: The schema to use, as returned by avro.schema.Parse
      codec: The codec to use for block-level compression. Any string supported
        by the Avro specification is accepted (for example 'null').
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
      use_fastavro: when set, use the `fastavro` library for IO

    Returns:
      A WriteToAvro transform usable for writing.
    """
    self._sink = \
      _create_avro_sink(
          file_path_prefix,
          schema,
          codec,
          file_name_suffix,
          num_shards,
          shard_name_template,
          mime_type,
          use_fastavro
      )

  def expand(self, pcoll):
    return pcoll | beam.io.iobase.Write(self._sink)

  def display_data(self):
    return {'sink_dd': self._sink}


def _create_avro_sink(file_path_prefix,
                      schema,
                      codec,
                      file_name_suffix,
                      num_shards,
                      shard_name_template,
                      mime_type,
                      use_fastavro):
  return \
      _FastAvroSink(
          file_path_prefix,
          schema,
          codec,
          file_name_suffix,
          num_shards,
          shard_name_template,
          mime_type
      ) \
      if use_fastavro \
      else \
      _AvroSink(
          file_path_prefix,
          schema,
          codec,
          file_name_suffix,
          num_shards,
          shard_name_template,
          mime_type
      )


class _AvroSink(filebasedsink.FileBasedSink):
  """A sink for avro files."""

  def __init__(self,
               file_path_prefix,
               schema,
               codec,
               file_name_suffix,
               num_shards,
               shard_name_template,
               mime_type):
    super(_AvroSink, self).__init__(
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

  def open(self, temp_path):
    file_handle = super(_AvroSink, self).open(temp_path)
    return avro.datafile.DataFileWriter(
        file_handle, avro.io.DatumWriter(), self._schema, self._codec)

  def write_record(self, writer, value):
    writer.append(value)

  def display_data(self):
    res = super(_AvroSink, self).display_data()
    res['codec'] = str(self._codec)
    res['schema'] = str(self._schema)
    return res


class _FastAvroSink(_AvroSink):
  """A sink for avro files that uses the `fastavro` library"""
  def open(self, temp_path):
    file_handle = super(_AvroSink, self).open(temp_path)
    return Writer(file_handle, self._schema.to_json(), self._codec)

  def write_record(self, writer, value):
    writer.write(value)

  def close(self, writer):
    writer.flush()
    super(_FastAvroSink, self).close(writer.fo)
