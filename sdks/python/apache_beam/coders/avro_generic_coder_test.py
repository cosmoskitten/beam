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
from __future__ import absolute_import

import logging
import unittest

from apache_beam.coders.avro_generic_coder import AvroGenericCoder
from apache_beam.coders.avro_generic_coder import AvroGenericRecord
from apache_beam.coders.typecoders import registry as coders_registry


class AvroGenericTestCoder(AvroGenericCoder):
  SCHEMA = """
  {
    "type": "record", "name": "testrecord",
    "fields": [
      {"name": "name", "type": "string"},
      {"name": "age", "type": "int"}
    ]
  }
  """

  def __init__(self):
    super(AvroGenericTestCoder, self).__init__(self.SCHEMA)


class AvroGenericTestRecord(AvroGenericRecord):
  pass


coders_registry.register_coder(AvroGenericTestRecord, AvroGenericTestCoder)


class CodersTest(unittest.TestCase):

  def test_avro_generic_record_coder(self):
    real_coder = coders_registry.get_coder(AvroGenericTestRecord)
    expected_coder = AvroGenericTestCoder()
    self.assertEqual(
        real_coder.encode(
            AvroGenericTestRecord({"name": "Daenerys targaryen", "age": 23})),
        expected_coder.encode(
            AvroGenericTestRecord({"name": "Daenerys targaryen", "age": 23}))
    )
    self.assertEqual(
        AvroGenericTestRecord({"name": "Jon Snow", "age": 23}),
        real_coder.decode(
            real_coder.encode(
                AvroGenericTestRecord({"name": "Jon Snow", "age": 23}))
        )
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
