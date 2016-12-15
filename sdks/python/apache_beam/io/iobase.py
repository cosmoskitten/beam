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

"""Sources and sinks.

A Source manages record-oriented data input from a particular kind of source
(e.g. a set of files, a database table, etc.). The reader() method of a source
returns a reader object supporting the iterator protocol; iteration yields
raw records of unprocessed, serialized data.


A Sink manages record-oriented data output to a particular kind of sink
(e.g. a set of files, a database table, etc.). The writer() method of a sink
returns a writer object supporting writing records of serialized data to
the sink.
"""

from collections import namedtuple

import logging
import random
import uuid

from apache_beam import pvalue
from apache_beam import coders
from apache_beam.pvalue import AsIter
from apache_beam.pvalue import AsSingleton
from apache_beam.transforms import core
from apache_beam.transforms import ptransform
from apache_beam.transforms import window
from apache_beam.transforms.display import HasDisplayData
from apache_beam.transforms.display import DisplayDataItem


# Encapsulates information about a bundle of a source generated when method
# BoundedSource.split() is invoked.
# This is a named 4-tuple that has following fields.
# * weight - a number that represents the size of the bundle. This value will
#            be used to compare the relative sizes of bundles generated by the
#            current source.
#            The weight returned here could be specified using a unit of your
#            choice (for example, bundles of sizes 100MB, 200MB, and 700MB may
#            specify weights 100, 200, 700 or 1, 2, 7) but all bundles of a
#            source should specify the weight using the same unit.
# * source - a BoundedSource object for the  bundle.
# * start_position - starting position of the bundle
# * stop_position - ending position of the bundle.
#
# Type for start and stop positions are specific to the bounded source and must
# be consistent throughout.
SourceBundle = namedtuple(
    'SourceBundle',
    'weight source start_position stop_position')


class BoundedSource(HasDisplayData):
  """A source that reads a finite amount of input records.

  This class defines following operations which can be used to read the source
  efficiently.

  * Size estimation - method ``estimate_size()`` may return an accurate
    estimation in bytes for the size of the source.
  * Splitting into bundles of a given size - method ``split()`` can be used to
    split the source into a set of sub-sources (bundles) based on a desired
    bundle size.
  * Getting a RangeTracker - method ``get_range_tracker()`` should return a
    ``RangeTracker`` object for a given position range for the position type
    of the records returned by the source.
  * Reading the data - method ``read()`` can be used to read data from the
    source while respecting the boundaries defined by a given
    ``RangeTracker``.

  A runner will perform reading the source in two steps.
  (1) Method ``get_range_tracker()`` will be invoked with start and end
      positions to obtain a ``RangeTracker`` for the range of positions the
      runner intends to read. Source must define a default initial start and end
      position range. These positions must be used if the start and/or end
      positions passed to the method ``get_range_tracker()`` are ``None``
  (2) Method read() will be invoked with the ``RangeTracker`` obtained in the
      previous step.

  **Mutability**

  A ``BoundedSource`` object should not be mutated while
  its methods (for example, ``read()``) are being invoked by a runner. Runner
  implementations may invoke methods of ``BoundedSource`` objects through
  multi-threaded and/or reentrant execution modes.
  """

  def estimate_size(self):
    """Estimates the size of source in bytes.

    An estimate of the total size (in bytes) of the data that would be read
    from this source. This estimate is in terms of external storage size,
    before performing decompression or other processing.

    Returns:
      estimated size of the source if the size can be determined, ``None``
      otherwise.
    """
    raise NotImplementedError

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    """Splits the source into a set of bundles.

    Bundles should be approximately of size ``desired_bundle_size`` bytes.

    Args:
      desired_bundle_size: the desired size (in bytes) of the bundles returned.
      start_position: if specified the given position must be used as the
                      starting position of the first bundle.
      stop_position: if specified the given position must be used as the ending
                     position of the last bundle.
    Returns:
      an iterator of objects of type 'SourceBundle' that gives information about
      the generated bundles.
    """
    raise NotImplementedError

  def get_range_tracker(self, start_position, stop_position):
    """Returns a RangeTracker for a given position range.

    Framework may invoke ``read()`` method with the RangeTracker object returned
    here to read data from the source.
    Args:
      start_position: starting position of the range. If 'None' default start
                      position of the source must be used.
      stop_position:  ending position of the range. If 'None' default stop
                      position of the source must be used.
    Returns:
      a ``RangeTracker`` for the given position range.
    """
    raise NotImplementedError

  def read(self, range_tracker):
    """Returns an iterator that reads data from the source.

    The returned set of data must respect the boundaries defined by the given
    ``RangeTracker`` object. For example:

      * Returned set of data must be for the range
        ``[range_tracker.start_position, range_tracker.stop_position)``. Note
        that a source may decide to return records that start after
        ``range_tracker.stop_position``. See documentation in class
        ``RangeTracker`` for more details. Also, note that framework might
        invoke ``range_tracker.try_split()`` to perform dynamic split
        operations. range_tracker.stop_position may be updated
        dynamically due to successful dynamic split operations.
      * Method ``range_tracker.try_split()`` must be invoked for every record
        that starts at a split point.
      * Method ``range_tracker.record_current_position()`` may be invoked for
        records that do not start at split points.

    Args:
      range_tracker: a ``RangeTracker`` whose boundaries must be respected
                     when reading data from the source. A runner that reads this
                     source muss pass a ``RangeTracker`` object that is not
                     ``None``.
    Returns:
      an iterator of data read by the source.
    """
    raise NotImplementedError

  def default_output_coder(self):
    """Coder that should be used for the records returned by the source.

    Should be overridden by sources that produce objects that can be encoded
    more efficiently than pickling.
    """
    return coders.registry.get_coder(object)


class RangeTracker(object):
  """A thread safe object used by Dataflow source framework.

  A Dataflow source is defined using a ''BoundedSource'' and a ''RangeTracker''
  pair. A ''RangeTracker'' is used by Dataflow source framework to perform
  dynamic work rebalancing of position-based sources.

  **Position-based sources**

  A position-based source is one where the source can be described by a range
  of positions of an ordered type and the records returned by the reader can be
  described by positions of the same type.

  In case a record occupies a range of positions in the source, the most
  important thing about the record is the position where it starts.

  Defining the semantics of positions for a source is entirely up to the source
  class, however the chosen definitions have to obey certain properties in order
  to make it possible to correctly split the source into parts, including
  dynamic splitting. Two main aspects need to be defined:

  1. How to assign starting positions to records.
  2. Which records should be read by a source with a range '[A, B)'.

  Moreover, reading a range must be *efficient*, i.e., the performance of
  reading a range should not significantly depend on the location of the range.
  For example, reading the range [A, B) should not require reading all data
  before 'A'.

  The sections below explain exactly what properties these definitions must
  satisfy, and how to use a ``RangeTracker`` with a properly defined source.

  **Properties of position-based sources**

  The main requirement for position-based sources is *associativity*: reading
  records from '[A, B)' and records from '[B, C)' should give the same
  records as reading from '[A, C)', where 'A <= B <= C'. This property
  ensures that no matter how a range of positions is split into arbitrarily many
  sub-ranges, the total set of records described by them stays the same.

  The other important property is how the source's range relates to positions of
  records in the source. In many sources each record can be identified by a
  unique starting position. In this case:

  * All records returned by a source '[A, B)' must have starting positions in
    this range.
  * All but the last record should end within this range. The last record may or
    may not extend past the end of the range.
  * Records should not overlap.

  Such sources should define "read '[A, B)'" as "read from the first record
  starting at or after 'A', up to but not including the first record starting
  at or after 'B'".

  Some examples of such sources include reading lines or CSV from a text file,
  reading keys and values from a BigTable, etc.

  The concept of *split points* allows to extend the definitions for dealing
  with sources where some records cannot be identified by a unique starting
  position.

  In all cases, all records returned by a source '[A, B)' must *start* at or
  after 'A'.

  **Split points**

  Some sources may have records that are not directly addressable. For example,
  imagine a file format consisting of a sequence of compressed blocks. Each
  block can be assigned an offset, but records within the block cannot be
  directly addressed without decompressing the block. Let us refer to this
  hypothetical format as <i>CBF (Compressed Blocks Format)</i>.

  Many such formats can still satisfy the associativity property. For example,
  in CBF, reading '[A, B)' can mean "read all the records in all blocks whose
  starting offset is in '[A, B)'".

  To support such complex formats, we introduce the notion of *split points*. We
  say that a record is a split point if there exists a position 'A' such that
  the record is the first one to be returned when reading the range
  '[A, infinity)'. In CBF, the only split points would be the first records
  in each block.

  Split points allow us to define the meaning of a record's position and a
  source's range in all cases:

  * For a record that is at a split point, its position is defined to be the
    largest 'A' such that reading a source with the range '[A, infinity)'
    returns this record.
  * Positions of other records are only required to be non-decreasing.
  * Reading the source '[A, B)' must return records starting from the first
    split point at or after 'A', up to but not including the first split point
    at or after 'B'. In particular, this means that the first record returned
    by a source MUST always be a split point.
  * Positions of split points must be unique.

  As a result, for any decomposition of the full range of the source into
  position ranges, the total set of records will be the full set of records in
  the source, and each record will be read exactly once.

  **Consumed positions**

  As the source is being read, and records read from it are being passed to the
  downstream transforms in the pipeline, we say that positions in the source are
  being *consumed*. When a reader has read a record (or promised to a caller
  that a record will be returned), positions up to and including the record's
  start position are considered *consumed*.

  Dynamic splitting can happen only at *unconsumed* positions. If the reader
  just returned a record at offset 42 in a file, dynamic splitting can happen
  only at offset 43 or beyond, as otherwise that record could be read twice (by
  the current reader and by a reader of the task starting at 43).
  """

  def start_position(self):
    """Returns the starting position of the current range, inclusive."""
    raise NotImplementedError(type(self))

  def stop_position(self):
    """Returns the ending position of the current range, exclusive."""
    raise NotImplementedError(type(self))

  def try_claim(self, position):  # pylint: disable=unused-argument
    """Atomically determines if a record at a split point is within the range.

    This method should be called **if and only if** the record is at a split
    point. This method may modify the internal state of the ``RangeTracker`` by
    updating the last-consumed position to ``position``.

    ** Thread safety **

    This method along with several other methods of this class may be invoked by
    multiple threads, hence must be made thread-safe, e.g. by using a single
    lock object.

    Args:
      position: starting position of a record being read by a source.

    Returns:
      ``True``, if the given position falls within the current range, returns
      ``False`` otherwise.
    """
    raise NotImplementedError

  def set_current_position(self, position):
    """Updates the last-consumed position to the given position.

    A source may invoke this method for records that do not start at split
    points. This may modify the internal state of the ``RangeTracker``. If the
    record starts at a split point, method ``try_claim()`` **must** be invoked
    instead of this method.

    Args:
      position: starting position of a record being read by a source.
    """
    raise NotImplementedError

  def position_at_fraction(self, fraction):
    """Returns the position at the given fraction.

    Given a fraction within the range [0.0, 1.0) this method will return the
    position at the given fraction compared to the position range
    [self.start_position, self.stop_position).

    ** Thread safety **

    This method along with several other methods of this class may be invoked by
    multiple threads, hence must be made thread-safe, e.g. by using a single
    lock object.

    Args:
      fraction: a float value within the range [0.0, 1.0).
    Returns:
      a position within the range [self.start_position, self.stop_position).
    """
    raise NotImplementedError

  def try_split(self, position):
    """Atomically splits the current range.

    Determines a position to split the current range, split_position, based on
    the given position. In most cases split_position and position will be the
    same.

    Splits the current range '[self.start_position, self.stop_position)'
    into a "primary" part '[self.start_position, split_position)' and a
    "residual" part '[split_position, self.stop_position)', assuming the
    current last-consumed position is within
    '[self.start_position, split_position)' (i.e., split_position has not been
    consumed yet).

    If successful, updates the current range to be the primary and returns a
    tuple (split_position, split_fraction). split_fraction should be the
    fraction of size of range '[self.start_position, split_position)' compared
    to the original (before split) range
    '[self.start_position, self.stop_position)'.

    If the split_position has already been consumed, returns ``None``.

    ** Thread safety **

    This method along with several other methods of this class may be invoked by
    multiple threads, hence must be made thread-safe, e.g. by using a single
    lock object.

    Args:
      position: suggested position where the current range should try to
                be split at.
    Returns:
      a tuple containing the split position and split fraction if split is
      successful. Returns ``None`` otherwise.
    """
    raise NotImplementedError

  def fraction_consumed(self):
    """Returns the approximate fraction of consumed positions in the source.

    ** Thread safety **

    This method along with several other methods of this class may be invoked by
    multiple threads, hence must be made thread-safe, e.g. by using a single
    lock object.

    Returns:
      the approximate fraction of positions that have been consumed by
      successful 'try_split()' and  'report_current_position()'  calls, or
      0.0 if no such calls have happened.
    """
    raise NotImplementedError


class Sink(HasDisplayData):
  """A resource that can be written to using the ``df.io.Write`` transform.

  Here ``df`` stands for Dataflow Python code imported in following manner.
  ``import apache_beam as beam``.

  A parallel write to an ``iobase.Sink`` consists of three phases:

  1. A sequential *initialization* phase (e.g., creating a temporary output
     directory, etc.)
  2. A parallel write phase where workers write *bundles* of records
  3. A sequential *finalization* phase (e.g., committing the writes, merging
     output files, etc.)

  For exact definition of a Dataflow bundle please see
  https://cloud.google.com/dataflow/faq.

  Implementing a new sink requires extending two classes.

  1. iobase.Sink

  ``iobase.Sink`` is an immutable logical description of the location/resource
  to write to. Depending on the type of sink, it may contain fields such as the
  path to an output directory on a filesystem, a database table name,
  etc. ``iobase.Sink`` provides methods for performing a write operation to the
  sink described by it. To this end, implementors of an extension of
  ``iobase.Sink`` must implement three methods:
  ``initialize_write()``, ``open_writer()``, and ``finalize_write()``.

  2. iobase.Writer

  ``iobase.Writer`` is used to write a single bundle of records. An
  ``iobase.Writer`` defines two methods: ``write()`` which writes a
  single record from the bundle and ``close()`` which is called once
  at the end of writing a bundle.

  See also ``df.io.fileio.FileSink`` which provides a simpler API for writing
  sinks that produce files.

  **Execution of the Write transform**

  ``initialize_write()`` and ``finalize_write()`` are conceptually called once:
  at the beginning and end of a ``Write`` transform. However, implementors must
  ensure that these methods are *idempotent*, as they may be called multiple
  times on different machines in the case of failure/retry or for redundancy.

  ``initialize_write()`` should perform any initialization that needs to be done
  prior to writing to the sink. ``initialize_write()`` may return a result
  (let's call this ``init_result``) that contains any parameters it wants to
  pass on to its writers about the sink. For example, a sink that writes to a
  file system may return an ``init_result`` that contains a dynamically
  generated unique directory to which data should be written.

  To perform writing of a bundle of elements, Dataflow execution engine will
  create an ``iobase.Writer`` using the implementation of
  ``iobase.Sink.open_writer()``. When invoking ``open_writer()`` execution
  engine will provide the ``init_result`` returned by ``initialize_write()``
  invocation as well as a *bundle id* (let's call this ``bundle_id``) that is
  unique for each invocation of ``open_writer()``.

  Execution engine will then invoke ``iobase.Writer.write()`` implementation for
  each element that has to be written. Once all elements of a bundle are
  written, execution engine will invoke ``iobase.Writer.close()`` implementation
  which should return a result (let's call this ``write_result``) that contains
  information that encodes the result of the write and, in most cases, some
  encoding of the unique bundle id. For example, if each bundle is written to a
  unique temporary file, ``close()`` method may return an object that contains
  the temporary file name. After writing of all bundles is complete, execution
  engine will invoke ``finalize_write()`` implementation. As parameters to this
  invocation execution engine will provide ``init_result`` as well as an
  iterable of ``write_result``.

  The execution of a write transform can be illustrated using following pseudo
  code (assume that the outer for loop happens in parallel across many
  machines)::

    init_result = sink.initialize_write()
    write_results = []
    for bundle in partition(pcoll):
      writer = sink.open_writer(init_result, generate_bundle_id())
      for elem in bundle:
        writer.write(elem)
      write_results.append(writer.close())
    sink.finalize_write(init_result, write_results)


  **init_result**

  Methods of 'iobase.Sink' should agree on the 'init_result' type that will be
  returned when initializing the sink. This type can be a client-defined object
  or an existing type. The returned type must be picklable using Dataflow coder
  ``coders.PickleCoder``. Returning an init_result is optional.

  **bundle_id**

  In order to ensure fault-tolerance, a bundle may be executed multiple times
  (e.g., in the event of failure/retry or for redundancy). However, exactly one
  of these executions will have its result passed to the
  ``iobase.Sink.finalize_write()`` method. Each call to
  ``iobase.Sink.open_writer()`` is passed a unique bundle id when it is called
  by the ``WriteImpl`` transform, so even redundant or retried bundles will have
  a unique way of identifying their output.

  The bundle id should be used to guarantee that a bundle's output is unique.
  This uniqueness guarantee is important; if a bundle is to be output to a file,
  for example, the name of the file must be unique to avoid conflicts with other
  writers. The bundle id should be encoded in the writer result returned by the
  writer and subsequently used by the ``finalize_write()`` method to identify
  the results of successful writes.

  For example, consider the scenario where a Writer writes files containing
  serialized records and the ``finalize_write()`` is to merge or rename these
  output files. In this case, a writer may use its unique id to name its output
  file (to avoid conflicts) and return the name of the file it wrote as its
  writer result. The ``finalize_write()`` will then receive an ``Iterable`` of
  output file names that it can then merge or rename using some bundle naming
  scheme.

  **write_result**

  ``iobase.Writer.close()`` and ``finalize_write()`` implementations must agree
  on type of the ``write_result`` object returned when invoking
  ``iobase.Writer.close()``. This type can be a client-defined object or
  an existing type. The returned type must be picklable using Dataflow coder
  ``coders.PickleCoder``. Returning a ``write_result`` when
  ``iobase.Writer.close()`` is invoked is optional but if unique
  ``write_result`` objects are not returned, sink should, guarantee idempotency
  when same bundle is written multiple times due to failure/retry or redundancy.


  **More information**

  For more information on creating new sinks please refer to the official
  documentation at
  ``https://cloud.google.com/dataflow/model/custom-io#creating-sinks``.
  """

  def initialize_write(self):
    """Initializes the sink before writing begins.

    Invoked before any data is written to the sink.


    Please see documentation in ``iobase.Sink`` for an example.

    Returns:
      An object that contains any sink specific state generated by
      initialization. This object will be passed to open_writer() and
      finalize_write() methods.
    """
    raise NotImplementedError

  def open_writer(self, init_result, uid):
    """Opens a writer for writing a bundle of elements to the sink.

    Args:
      init_result: the result of initialize_write() invocation.
      uid: a unique identifier generated by the system.
    Returns:
      an ``iobase.Writer`` that can be used to write a bundle of records to the
      current sink.
    """
    raise NotImplementedError

  def finalize_write(self, init_result, writer_results):
    """Finalizes the sink after all data is written to it.

    Given the result of initialization and an iterable of results from bundle
    writes, performs finalization after writing and closes the sink. Called
    after all bundle writes are complete.

    The bundle write results that are passed to finalize are those returned by
    bundles that completed successfully. Although bundles may have been run
    multiple times (for fault-tolerance), only one writer result will be passed
    to finalize for each bundle. An implementation of finalize should perform
    clean up of any failed and successfully retried bundles.  Note that these
    failed bundles will not have their writer result passed to finalize, so
    finalize should be capable of locating any temporary/partial output written
    by failed bundles.

    If all retries of a bundle fails, the whole pipeline will fail *without*
    finalize_write() being invoked.

    A best practice is to make finalize atomic. If this is impossible given the
    semantics of the sink, finalize should be idempotent, as it may be called
    multiple times in the case of failure/retry or for redundancy.

    Note that the iteration order of the writer results is not guaranteed to be
    consistent if finalize is called multiple times.

    Args:
      init_result: the result of ``initialize_write()`` invocation.
      writer_results: an iterable containing results of ``Writer.close()``
        invocations. This will only contain results of successful writes, and
        will only contain the result of a single successful write for a given
        bundle.
    """
    raise NotImplementedError


class Writer(object):
  """Writes a bundle of elements from a ``PCollection`` to a sink.

  A Writer  ``iobase.Writer.write()`` writes and elements to the sink while
  ``iobase.Writer.close()`` is called after all elements in the bundle have been
  written.

  See ``iobase.Sink`` for more detailed documentation about the process of
  writing to a sink.
  """

  def write(self, value):
    """Writes a value to the sink using the current writer."""
    raise NotImplementedError

  def close(self):
    """Closes the current writer.

    Please see documentation in ``iobase.Sink`` for an example.

    Returns:
      An object representing the writes that were performed by the current
      writer.
    """
    raise NotImplementedError


class Read(ptransform.PTransform):
  """A transform that reads a PCollection."""

  def __init__(self, *args, **kwargs):
    """Initializes a Read transform.

    Args:
      *args: A tuple of position arguments.
      **kwargs: A dictionary of keyword arguments.

    The *args, **kwargs are expected to be (label, source) or (source).
    """
    label, source = self.parse_label_and_arg(args, kwargs, 'source')
    super(Read, self).__init__(label)
    self.source = source

  def expand(self, pbegin):
    assert isinstance(pbegin, pvalue.PBegin)
    self.pipeline = pbegin.pipeline
    return pvalue.PCollection(self.pipeline)

  def get_windowing(self, unused_inputs):
    return core.Windowing(window.GlobalWindows())

  def _infer_output_coder(self, input_type=None, input_coder=None):
    if isinstance(self.source, BoundedSource):
      return self.source.default_output_coder()
    else:
      return self.source.coder

  def display_data(self):
    return {'source': DisplayDataItem(self.source.__class__,
                                      label='Read Source'),
            'source_dd': self.source}


class Write(ptransform.PTransform):
  """A ``PTransform`` that writes to a sink.

  A sink should inherit ``iobase.Sink``. Such implementations are
  handled using a composite transform that consists of three ``ParDo``s -
  (1) a ``ParDo`` performing a global initialization (2) a ``ParDo`` performing
  a parallel write and (3) a ``ParDo`` performing a global finalization. In the
  case of an empty ``PCollection``, only the global initialization and
  finalization will be performed. Currently only batch workflows support custom
  sinks.

  Example usage::

      pcollection | beam.io.Write(MySink())

  This returns a ``pvalue.PValue`` object that represents the end of the
  Pipeline.

  The sink argument may also be a full PTransform, in which case it will be
  applied directly.  This allows composite sink-like transforms (e.g. a sink
  with some pre-processing DoFns) to be used the same as all other sinks.

  This transform also supports sinks that inherit ``iobase.NativeSink``. These
  are sinks that are implemented natively by the Dataflow service and hence
  should not be updated by users. These sinks are processed using a Dataflow
  native write transform.
  """

  def __init__(self, *args, **kwargs):
    """Initializes a Write transform.

    Args:
      *args: A tuple of position arguments.
      **kwargs: A dictionary of keyword arguments.

    The *args, **kwargs are expected to be (label, sink) or (sink).
    """
    label, sink = self.parse_label_and_arg(args, kwargs, 'sink')
    super(Write, self).__init__(label)
    self.sink = sink

  def display_data(self):
    return {'sink': self.sink.__class__,
            'sink_dd': self.sink}

  def expand(self, pcoll):
    from apache_beam.runners.dataflow.native_io import iobase as dataflow_io
    if isinstance(self.sink, dataflow_io.NativeSink):
      # A native sink
      return pcoll | 'native_write' >> dataflow_io._NativeWrite(self.sink)
    elif isinstance(self.sink, Sink):
      # A custom sink
      return pcoll | WriteImpl(self.sink)
    elif isinstance(self.sink, ptransform.PTransform):
      # This allows "composite" sinks to be used like non-composite ones.
      return pcoll | self.sink
    else:
      raise ValueError('A sink must inherit iobase.Sink, iobase.NativeSink, '
                       'or be a PTransform. Received : %r', self.sink)


class WriteImpl(ptransform.PTransform):
  """Implements the writing of custom sinks."""

  def __init__(self, sink):
    super(WriteImpl, self).__init__()
    self.sink = sink

  def expand(self, pcoll):
    do_once = pcoll.pipeline | 'DoOnce' >> core.Create([None])
    init_result_coll = do_once | core.Map(
        'initialize_write', lambda _, sink: sink.initialize_write(), self.sink)
    if getattr(self.sink, 'num_shards', 0):
      min_shards = self.sink.num_shards
      if min_shards == 1:
        keyed_pcoll = pcoll | core.Map(lambda x: (None, x))
      else:
        keyed_pcoll = pcoll | core.ParDo(_RoundRobinKeyFn(min_shards))
      write_result_coll = (keyed_pcoll
                           | core.WindowInto(window.GlobalWindows())
                           | core.GroupByKey()
                           | core.Map('write_bundles',
                                      _write_keyed_bundle, self.sink,
                                      AsSingleton(init_result_coll)))
    else:
      min_shards = 1
      write_result_coll = (pcoll | core.ParDo('write_bundles',
                                              _WriteBundleDoFn(), self.sink,
                                              AsSingleton(init_result_coll))
                           | core.Map('pair', lambda x: (None, x))
                           | core.WindowInto(window.GlobalWindows())
                           | core.GroupByKey()
                           | core.FlatMap('extract', lambda x: x[1]))
    return do_once | core.FlatMap(
        'finalize_write',
        _finalize_write,
        self.sink,
        AsSingleton(init_result_coll),
        AsIter(write_result_coll),
        min_shards)


class _WriteBundleDoFn(core.DoFn):
  """A DoFn for writing elements to an iobase.Writer.

  Opens a writer at the first element and closes the writer at finish_bundle().
  """

  def __init__(self):
    self.writer = None

  def process(self, context, sink, init_result):
    if self.writer is None:
      self.writer = sink.open_writer(init_result, str(uuid.uuid4()))
    self.writer.write(context.element)

  def finish_bundle(self, context, *args, **kwargs):
    if self.writer is not None:
      yield window.TimestampedValue(self.writer.close(), window.MAX_TIMESTAMP)


def _write_keyed_bundle(bundle, sink, init_result):
  writer = sink.open_writer(init_result, str(uuid.uuid4()))
  for element in bundle[1]:  # values
    writer.write(element)
  return window.TimestampedValue(writer.close(), window.MAX_TIMESTAMP)


def _finalize_write(_, sink, init_result, write_results, min_shards):
  write_results = list(write_results)
  extra_shards = []
  if len(write_results) < min_shards:
    logging.debug(
        'Creating %s empty shard(s).', min_shards - len(write_results))
    for _ in range(min_shards - len(write_results)):
      writer = sink.open_writer(init_result, str(uuid.uuid4()))
      extra_shards.append(writer.close())
  outputs = sink.finalize_write(init_result, write_results + extra_shards)
  if outputs:
    return (window.TimestampedValue(v, window.MAX_TIMESTAMP) for v in outputs)


class _RoundRobinKeyFn(core.DoFn):

  def __init__(self, count):
    self.count = count

  def start_bundle(self, context):
    self.counter = random.randint(0, self.count - 1)

  def process(self, context):
    self.counter += 1
    if self.counter >= self.count:
      self.counter -= self.count
    yield self.counter, context.element


# For backwards compatibility.
# pylint: disable=wrong-import-position
from apache_beam.runners.dataflow.native_io.iobase import *
