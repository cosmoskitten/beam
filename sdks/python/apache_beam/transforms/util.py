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

"""Simple utility PTransforms.
"""

from __future__ import absolute_import

import collections
import contextlib
import time

from apache_beam import typehints
from apache_beam.transforms import window
from apache_beam.transforms.core import CombinePerKey
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.core import Flatten
from apache_beam.transforms.core import GroupByKey
from apache_beam.transforms.core import Map
from apache_beam.transforms.core import ParDo
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.ptransform import ptransform_fn
from apache_beam.utils import windowed_value

__all__ = [
    'CoGroupByKey',
    'Keys',
    'KvSwap',
    'RemoveDuplicates',
    'Values',
    ]


T = typehints.TypeVariable('T')


class CoGroupByKey(PTransform):
  """Groups results across several PCollections by key.

  Given an input dict mapping serializable keys (called "tags") to 0 or more
  PCollections of (key, value) tuples, e.g.::

     {'pc1': pcoll1, 'pc2': pcoll2, 33333: pcoll3}

  creates a single output PCollection of (key, value) tuples whose keys are the
  unique input keys from all inputs, and whose values are dicts mapping each
  tag to an iterable of whatever values were under the key in the corresponding
  PCollection::

    ('some key', {'pc1': ['value 1 under "some key" in pcoll1',
                          'value 2 under "some key" in pcoll1'],
                  'pc2': [],
                  33333: ['only value under "some key" in pcoll3']})

  Note that pcoll2 had no values associated with "some key".

  CoGroupByKey also works for tuples, lists, or other flat iterables of
  PCollections, in which case the values of the resulting PCollections
  will be tuples whose nth value is the list of values from the nth
  PCollection---conceptually, the "tags" are the indices into the input.
  Thus, for this input::

     (pcoll1, pcoll2, pcoll3)

  the output PCollection's value for "some key" is::

    ('some key', (['value 1 under "some key" in pcoll1',
                   'value 2 under "some key" in pcoll1'],
                  [],
                  ['only value under "some key" in pcoll3']))

  Args:
    label: name of this transform instance. Useful while monitoring and
      debugging a pipeline execution.
    **kwargs: Accepts a single named argument "pipeline", which specifies the
      pipeline that "owns" this PTransform. Ordinarily CoGroupByKey can obtain
      this information from one of the input PCollections, but if there are none
      (or if there's a chance there may be none), this argument is the only way
      to provide pipeline information, and should be considered mandatory.
  """

  def __init__(self, **kwargs):
    super(CoGroupByKey, self).__init__()
    self.pipeline = kwargs.pop('pipeline', None)
    if kwargs:
      raise ValueError('Unexpected keyword arguments: %s' % kwargs.keys())

  def _extract_input_pvalues(self, pvalueish):
    try:
      # If this works, it's a dict.
      return pvalueish, tuple(pvalueish.viewvalues())
    except AttributeError:
      pcolls = tuple(pvalueish)
      return pcolls, pcolls

  def expand(self, pcolls):
    """Performs CoGroupByKey on argument pcolls; see class docstring."""
    # For associating values in K-V pairs with the PCollections they came from.
    def _pair_tag_with_value((key, value), tag):
      return (key, (tag, value))

    # Creates the key, value pairs for the output PCollection. Values are either
    # lists or dicts (per the class docstring), initialized by the result of
    # result_ctor(result_ctor_arg).
    def _merge_tagged_vals_under_key((key, grouped), result_ctor,
                                     result_ctor_arg):
      result_value = result_ctor(result_ctor_arg)
      for tag, value in grouped:
        result_value[tag].append(value)
      return (key, result_value)

    try:
      # If pcolls is a dict, we turn it into (tag, pcoll) pairs for use in the
      # general-purpose code below. The result value constructor creates dicts
      # whose keys are the tags.
      result_ctor_arg = pcolls.keys()
      result_ctor = lambda tags: dict((tag, []) for tag in tags)
      pcolls = pcolls.items()
    except AttributeError:
      # Otherwise, pcolls is a list/tuple, so we turn it into (index, pcoll)
      # pairs. The result value constructor makes tuples with len(pcolls) slots.
      pcolls = list(enumerate(pcolls))
      result_ctor_arg = len(pcolls)
      result_ctor = lambda size: tuple([] for _ in xrange(size))

    # Check input PCollections for PCollection-ness, and that they all belong
    # to the same pipeline.
    for _, pcoll in pcolls:
      self._check_pcollection(pcoll)
      if self.pipeline:
        assert pcoll.pipeline == self.pipeline

    return ([pcoll | 'pair_with_%s' % tag >> Map(_pair_tag_with_value, tag)
             for tag, pcoll in pcolls]
            | Flatten(pipeline=self.pipeline)
            | GroupByKey()
            | Map(_merge_tagged_vals_under_key, result_ctor, result_ctor_arg))


def Keys(label='Keys'):  # pylint: disable=invalid-name
  """Produces a PCollection of first elements of 2-tuples in a PCollection."""
  return label >> Map(lambda (k, v): k)


def Values(label='Values'):  # pylint: disable=invalid-name
  """Produces a PCollection of second elements of 2-tuples in a PCollection."""
  return label >> Map(lambda (k, v): v)


def KvSwap(label='KvSwap'):  # pylint: disable=invalid-name
  """Produces a PCollection reversing 2-tuples in a PCollection."""
  return label >> Map(lambda (k, v): (v, k))


@ptransform_fn
def RemoveDuplicates(pcoll):  # pylint: disable=invalid-name
  """Produces a PCollection containing the unique elements of a PCollection."""
  return (pcoll
          | 'ToPairs' >> Map(lambda v: (v, None))
          | 'Group' >> CombinePerKey(lambda vs: None)
          | 'RemoveDuplicates' >> Keys())


class _BatchSizeEstimator(object):
  """Estimates the best size for batches given historical timing.
  """

  MAX_DATA_POINTS = 100
  MAX_GROWTH_FACTOR = 2

  def __init__(self,
               min_batch_size=1,
               max_batch_size=1000,
               target_batch_overhead=.1,
               target_batch_duration=1,
               clock=time.time):
    if min_batch_size > max_batch_size:
      raise ValueError("Minimum (%s) must not be greater than maximum (%s)" % (
          min_batch_size, max_batch_size))
    self._min_batch_size = min_batch_size
    self._max_batch_size = max_batch_size
    self._target_batch_overhead = target_batch_overhead
    self._target_batch_duration = target_batch_duration
    self._clock = clock
    self._data = []

  @contextlib.contextmanager
  def record_time(self, batch_size):
    start = self._clock()
    yield
    self._data.append((batch_size, self._clock() - start))
    if len(self._data) >= self.MAX_DATA_POINTS:
      self._thin_data()

  def _thin_data(self):
    sorted_data = sorted(self._data)
    odd_one_out = [sorted_data[-1]] if len(sorted_data) % 2 == 1 else []
    # Sort the pairs by how different they are.
    pairs = sorted(zip(sorted_data[::2], sorted_data[1::2]),
                   key=lambda ((x1, _1), (x2, _2)): x2 / x1)
    # Keep the top 1/3 most different pairs, average the top 2/3 most similar.
    threshold = 2 * len(pairs) / 3
    self._data = (
        list(sum(pairs[threshold:], ()))
        + [((x1 + x2) / 2.0, (t1 + t2) / 2.0)
           for (x1, t1), (x2, t2) in pairs[:threshold]]
        + odd_one_out)

  def next_batch_size(self):
    if self._min_batch_size == self._max_batch_size:
      return self._min_batch_size
    elif len(self._data) < 1:
      return self._min_batch_size
    elif len(self._data) < 2:
      # Force some variety.
      return int(max(
          min(self._max_batch_size,
              self._min_batch_size * self.MAX_GROWTH_FACTOR),
          self._min_batch_size + 1))

    # Linear regression for y = a + bx, where x is batch size and y is time.
    xs, ys = zip(*self._data)
    n = float(len(self._data))
    xbar = sum(xs) / n
    ybar = sum(ys) / n
    b = (sum([(x - xbar) * (y - ybar) for x, y in self._data])
         / sum([(x - xbar)**2 for x in xs]))
    a = ybar - b * xbar

    # Avoid nonsensical or division-by-zero errors below due to noise.
    a = max(a, 1e-10)
    b = max(b, 1e-20)

    last_batch_size = self._data[-1][0]
    cap = min(last_batch_size * self.MAX_GROWTH_FACTOR, self._max_batch_size)

    if self._target_batch_duration:
      # Solution to a + b*x = self._target_batch_duration.
      cap = min(cap, (self._target_batch_duration - a) / b)

    if self._target_batch_overhead:
      # Solution to a / (a + b*x) = self._target_batch_overhead.
      cap = min(cap, (a / b) * (1 / self._target_batch_overhead - 1))

    # Avoid getting stuck at min_batch_size.
    jitter = len(self._data) % 2
    return int(max(self._min_batch_size + jitter, cap))


class _GlobalWindowsBatchingDoFn(DoFn):
  def __init__(self, batch_size_estimator):
    self._batch_size_estimator = batch_size_estimator

  def start_bundle(self):
    self._batch = []
    self._batch_size = self._batch_size_estimator.next_batch_size()
    self._first = True

  def process(self, element):
    self._batch.append(element)
    if len(self._batch) >= self._batch_size:
      if self._first:
        # This often involves non-trivial setup.
        yield self._batch
        self._first = False
      else:
        with self._batch_size_estimator.record_time(self._batch_size):
          yield self._batch
      self._batch = []
      self._batch_size = self._batch_size_estimator.next_batch_size()

  def finish_bundle(self):
    if self._batch:
      with self._batch_size_estimator.record_time(self._batch_size):
        yield window.GlobalWindows.windowed_value(self._batch)
      self._batch = None
      self._batch_size = self._batch_size_estimator.next_batch_size()


class _WindowAwareBatchingDoFn(DoFn):
  def __init__(self, batch_size_estimator):
    self._batch_size_estimator = batch_size_estimator

  def start_bundle(self):
    self._batches = collections.defaultdict(list)
    self._batch_size = self._batch_size_estimator.next_batch_size()
    self._first = True

  def process(self, element, window=DoFn.WindowParam):
    self._batches[window].append(element)
    if len(self._batches[window]) >= self._batch_size:
      windowed_batch = windowed_value.WindowedValue(
          self._batches[window], window.max_timestamp(), (window,))
      if self._first:
        # This often involves non-trivial setup.
        yield windowed_batch
        self._first = False
      else:
        with self._batch_size_estimator.record_time(self._batch_size):
          yield windowed_batch
      del self._batches[window]
      self._batch_size = self._batch_size_estimator.next_batch_size()

  def finish_bundle(self):
    for window, batch in self._batches.items():
      if batch:
        with self._batch_size_estimator.record_time(self._batch_size):
          yield windowed_value.WindowedValue(
              batch, window.max_timestamp(), (window,))
    self._batches = None
    self._batch_size = self._batch_size_estimator.next_batch_size()


@typehints.with_input_types(T)
@typehints.with_output_types(typehints.List[T])
class BatchElements(PTransform):
  """A Transform that batches elements for amortized processing.

  This transform is designed to proceed operations whose processing cost
  is of the form

      time = fixed_cost + num_elements * per_element_cost

  where the per element cost is (often significantly) smaller than the fixed
  cost.  It consumes a PCollection of T and produces a PCollection of List[T].

  This transform attempts to find the best batch size between the minimim
  and maximum parameters by profiling the time taken by (fused) downstream
  operations.
  For a fixed batch size, set the min and max to be equal.

  Args:
    min_batch_size: (optional) the smallest number of elements per batch
    max_batch_size: (optional) the largest number of elements per batch
    target_batch_overhead: (optional) a target for fixed_cost / time, as above
    target_batch_duration: (optional) a target for total time per bundle
    clock: (optional) an alternative to time.time for measuring the cost of
        donwstream operations (mostly for testing)
  """
  def __init__(self,
               min_batch_size=1,
               max_batch_size=1000,
               target_batch_overhead=.05,
               target_batch_duration=1,
               clock=time.time):
    self._batch_size_estimator = _BatchSizeEstimator(
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        target_batch_overhead=target_batch_overhead,
        target_batch_duration=target_batch_duration,
        clock=clock)

  def expand(self, pcoll):
    if getattr(pcoll.pipeline.runner, 'is_streaming', False):
      raise NotImplementedError("Requires stateful processing (BEAM-2687)")
    elif pcoll.windowing.is_default():
      return pcoll | ParDo(_GlobalWindowsBatchingDoFn(
          self._batch_size_estimator))
    else:
      return pcoll | ParDo(_WindowAwareBatchingDoFn(self._batch_size_estimator))
