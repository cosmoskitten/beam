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

    return DataclassTransform(**values)._payload

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
