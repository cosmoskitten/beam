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

"""A factory that creates UncommittedBundles."""

from __future__ import absolute_import

from apache_beam import pvalue
from apache_beam.utils.windowed_value import WindowedValue


class BundleFactory(object):
  """BundleFactory creates output bundles to be used by transform evaluators.

  Args:
    stacked: whether or not to stack the WindowedValues within the bundle
      in case consecutive ones share the same timestamp and windows.
      DirectRunnerOptions.direct_runner_use_stacked_bundle controls this option.
  """

  def __init__(self, stacked):
    self._stacked = stacked

  def create_bundle(self, output_pcollection):
    return Bundle(output_pcollection, self._stacked)

  def create_empty_committed_bundle(self, output_pcollection):
    bundle = self.create_bundle(output_pcollection)
    bundle.commit(None)
    return bundle


# a bundle represents a unit of work that will be processed by a transform.
class Bundle(object):
  """Part of a PCollection with output elements.

  Part of a PCollection. Elements are output to a bundle, which will cause them
  to be executed by PTransform that consume the PCollection this bundle is a
  part of at a later point. It starts as an uncommitted bundle and can have
  elements added to it. It needs to be committed to make it immutable before
  passing it to a downstream ptransform.

  The stored elements are WindowedValues, which contains timestamp and windows
  information.

  Bundle internally optimizes storage by stacking elements with the same
  timestamp and windows into StackedWindowedValues, and then returns an iterable
  to restore WindowedValues upon get_elements() call.

  When this optimization is not desired, it can be avoided by an option when
  creating bundles, like:::

    b = Bundle(stacked=False)
  """

  class StackedWindowedValues(object):
    """A stack of WindowedValues with the same timestamp and windows.

    It must be initialized from a single WindowedValue.

    Example:::

      s = StackedWindowedValues(windowed_value)
      if (another_windowed_value.timestamp == s.timestamp and
          another_windowed_value.windows == s.windows):
        s.add_value(another_windowed_value.value)
      windowed_values = [wv for wv in s.windowed_values()]
      # now windowed_values equals to [windowed_value, another_windowed_value]
    """

    def __init__(self, initial_windowed_value):
      self._initial_windowed_value = initial_windowed_value
      self._appended_values = []

    @property
    def timestamp(self):
      return self._initial_windowed_value.timestamp

    @property
    def windows(self):
      return self._initial_windowed_value.windows

    def add_value(self, value):
      self._appended_values.append(value)

    def windowed_values(self):
      # yield first windowed_value as is, then iterate through
      # _appended_values to yield WindowedValue on the fly.
      yield self._initial_windowed_value
      for v in self._appended_values:
        yield WindowedValue(v, self._initial_windowed_value.timestamp,
                            self._initial_windowed_value.windows)

  def __init__(self, pcollection, stacked=True):
    assert (isinstance(pcollection, pvalue.PCollection)
            or isinstance(pcollection, pvalue.PCollectionView))
    self._pcollection = pcollection
    self._elements = []
    self._stacked = stacked
    self._committed = False
    self._tag = None  # optional tag information for this bundle

  def get_elements_iterable(self, make_copy=False):
    """Returns iterable elements.

    Args:
      make_copy: whether to force returning copy or yielded iterable.

    Returns:
      unstacked elements,
      in the form of iterable if committed and make_copy is not True,
      or as a list of copied WindowedValues.
    """
    if not self._stacked:
      if self._committed and not make_copy:
        return self._elements
      else:
        return list(self._elements)

    def iterable_stacked_or_elements(elements):
      for e in elements:
        if isinstance(e, Bundle.StackedWindowedValues):
          for w in e.windowed_values():
            yield w
        else:
          yield e

    if self._committed and not make_copy:
      return iterable_stacked_or_elements(self._elements)
    else:
      # returns a copy.
      return [e for e in iterable_stacked_or_elements(self._elements)]

  def has_elements(self):
    return len(self._elements) > 0

  @property
  def tag(self):
    return self._tag

  @tag.setter
  def tag(self, value):
    assert not self._tag
    self._tag = value

  @property
  def pcollection(self):
    """PCollection that the elements of this UncommittedBundle belong to."""
    return self._pcollection

  def add(self, element):
    """Outputs an element to this bundle.

    Args:
      element: WindowedValue
    """
    assert not self._committed
    if not self._stacked:
      self._elements.append(element)
      return
    if (len(self._elements) > 0 and
        (isinstance(self._elements[-1], WindowedValue) or
         isinstance(self._elements[-1], Bundle.StackedWindowedValues)) and
        self._elements[-1].timestamp == element.timestamp and
        self._elements[-1].windows == element.windows):
      if isinstance(self._elements[-1], WindowedValue):
        self._elements[-1] = Bundle.StackedWindowedValues(self._elements[-1])
      self._elements[-1].add_value(element.value)
    else:
      self._elements.append(element)

  def output(self, element):
    self.add(element)

  def commit(self, synchronized_processing_time):
    """Commits this bundle.

    Uncommitted bundle will become committed (immutable) after this call.

    Args:
      synchronized_processing_time: the synchronized processing time at which
      this bundle was committed
    """
    assert not self._committed
    self._committed = True
    self._elements = tuple(self._elements)
    self._synchronized_processing_time = synchronized_processing_time
