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
"""Tests for filesystemio."""

# TODO: cleanup unused
import multiprocessing
import os
import threading
import unittest

from apache_beam.io import filesystemio


class TestPipeStream(unittest.TestCase):

  def _read_and_verify(self, stream, expected, buffer_size):
    data_list = []
    bytes_read = 0
    seen_last_block = False
    while True:
      data = stream.read(buffer_size)
      self.assertLessEqual(len(data), buffer_size)
      if len(data) < buffer_size:
        # Test the constraint that the pipe stream returns less than the buffer
        # size only when at the end of the stream.
        if data:
          self.assertFalse(seen_last_block)
        seen_last_block = True
      if not data:
        break
      data_list.append(data)
      bytes_read += len(data)
      self.assertEqual(stream.tell(), bytes_read)
    self.assertEqual(''.join(data_list), expected)

  def test_pipe_stream(self):
    block_sizes = list(4**i for i in range(0, 12))
    data_blocks = list(os.urandom(size) for size in block_sizes)
    expected = ''.join(data_blocks)

    buffer_sizes = [100001, 512 * 1024, 1024 * 1024]

    for buffer_size in buffer_sizes:
      parent_conn, child_conn = multiprocessing.Pipe()
      stream = filesystemio.BufferedWriter.PipeStream(child_conn)
      child_thread = threading.Thread(
          target=self._read_and_verify, args=(stream, expected, buffer_size))
      child_thread.start()
      for data in data_blocks:
        parent_conn.send_bytes(data)
      parent_conn.close()
      child_thread.join()


