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

"""Utilities for the Nexmark suite.

The Nexmark suite is a series of queries (streaming pipelines) performed
on a simulation of auction events. This util includes a Command class used
to terminate the streaming jobs launched in nexmark_launcher.py.

Usage:
To run a process for a certain duration, define in the code:
  command = Command(process_to_terminate, args)
  command.run(timeout=duration)

To test, run in a shell:
  $ python nexmark_util.py
"""
from __future__ import print_function

import logging
import threading
import time
from multiprocessing import Process


logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s')


class Command(object):
  def __init__(self, cmd, args, rc_cb=None):
    self.cmd = cmd
    self.args = args
    self.process = None
    self.rc_cb = rc_cb

  def run(self, timeout):
    def thread_target():
      logging.debug('Starting thread for %d seconds: %s',
                    timeout, self.cmd.__name__)

      self.process = Process(target=self.cmd, args=self.args)
      self.process.start()
      self.process.join()

      logging.info('%d seconds elapsed. Thread (%s) finished.',
                   timeout, self.cmd.__name__)

    thread = threading.Thread(target=thread_target, name='Thread-timeout')
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
      logging.info('Terminating process...')
      if self.process.is_alive():
        self.process.terminate()
      thread.join()
    if self.rc_cb:
      self.rc_cb(self.process.returncode)


if __name__ == '__main__':
  def timed_function(data):
    while True:
      print(data)
      time.sleep(1)
  command = Command(timed_function, ['.'])

  try:
    command.run(timeout=3)
  except Exception as exp:
    print('command failded with', exp)
