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
from __future__ import print_function

import socket
import sys


def pick_unused_port():
  return pick_unused_ports(num_ports=1)[0]


def pick_unused_ports(num_ports):
  """Returns num_ports currently unused ports on the localhost."""
  sockets = []
  ports = []
  for _ in range(0, num_ports):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sockets.append(s)
    s.bind(('localhost', 0))
    _, port = s.getsockname()
    ports.append(port)
  try:
    return ports
  finally:
    for s in sockets:
      s.close()


if __name__ == '__main__':
  num_ports = 1
  if len(sys.argv) >= 2:
    try:
      num_ports = int(sys.argv[1])
    except ValueError:
      print("Invalid argument '{}' (expected integer)".format(sys.argv[1]))
      print("Usage: python ports.py [num_ports]")
      sys.exit(1)
  for port in pick_unused_ports(num_ports):
    print(port)
