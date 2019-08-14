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

"""Unit tests for the transform.external classes."""

from __future__ import absolute_import

import typing
import unittest

import apache_beam as beam
from apache_beam import typehints
from apache_beam.transforms.external import AnnotationBasedPayloadBuilder
from apache_beam.transforms.external import DataclassBasedPayloadBuilder
from apache_beam.transforms.external_test import PayloadBase

# pylint: disable=wrong-import-order, wrong-import-position
try:
  import dataclasses
except ImportError:
  dataclasses = None
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(dataclasses is None, 'dataclasses library not available')
class ExternalDataclassesPayloadTest(PayloadBase, unittest.TestCase):

  def get_typing_payload(self, values):

    @dataclasses.dataclass
    class DataclassTransform(beam.ExternalTransform):
      URN = 'beam:external:fakeurn:v1'

      integer_example: int
      string_example: str
      list_of_strings: typing.List[str]
      optional_kv: typing.Optional[typing.Tuple[str, float]] = None
      optional_integer: typing.Optional[int] = None
      expansion_service: dataclasses.InitVar[typing.Optional[str]] = None

    return ExternalConfigurationPayloadDataclassTransform(**values)._payload

  def get_typehints_payload(self, values):

    @dataclasses.dataclass
    class DataclassTransform(beam.ExternalTransform):
      URN = 'beam:external:fakeurn:v1'

      integer_example: int
      string_example: str
      list_of_strings: typehints.List[str]
      optional_kv: typehints.Optional[typehints.KV[str, float]] = None
      optional_integer: typehints.Optional[int] = None
      expansion_service: dataclasses.InitVar[typehints.Optional[str]] = None

    return DataclassTransform(**values)._payload


class ExternalAnnotationPayloadTest(PayloadBase, unittest.TestCase):

  def get_typing_payload(self, values):
    class AnnotatedTransform(beam.ExternalTransform):
      URN = 'beam:external:fakeurn:v1'

      def __init__(self,
                   integer_example: int,
                   string_example: str,
                   list_of_strings: typing.List[str],
                   optional_kv: typing.Optional[typing.Tuple[str, float]] = None,
                   optional_integer: typing.Optional[int] = None,
                   expansion_service=None):
        super(AnnotatedTransform, self).__init__(
            self.URN,
            AnnotationBasedPayloadBuilder(
                self,
                integer_example=integer_example,
                string_example=string_example,
                list_of_strings=list_of_strings,
                optional_kv=optional_kv,
                optional_integer=optional_integer,
            ),
            expansion_service
        )

    return AnnotatedTransform(**values)._payload

  def get_typehints_payload(self, values):
    class AnnotatedTransform(beam.ExternalTransform):
      URN = 'beam:external:fakeurn:v1'

      def __init__(self,
                   integer_example: int,
                   string_example: str,
                   list_of_strings: typehints.List[str],
                   optional_kv: typehints.Optional[typehints.KV[str, float]] = None,
                   optional_integer: typehints.Optional[int] = None,
                   expansion_service=None):
        super(AnnotatedTransform, self).__init__(
            self.URN,
            AnnotationBasedPayloadBuilder(
                self,
                integer_example=integer_example,
                string_example=string_example,
                list_of_strings=list_of_strings,
                optional_kv=optional_kv,
                optional_integer=optional_integer,
            ),
            expansion_service
        )

    return AnnotatedTransform(**values)._payload

if __name__ == '__main__':
  unittest.main()
