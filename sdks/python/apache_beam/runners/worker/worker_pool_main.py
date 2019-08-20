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
"""Worker pool entry point."""

from __future__ import absolute_import

import argparse
import atexit
import logging
import subprocess
import sys
import threading
import time
from concurrent import futures

import grpc

from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.runners.worker import sdk_worker


class BeamFnExternalWorkerPoolServicer(
    beam_fn_api_pb2_grpc.BeamFnExternalWorkerPoolServicer):

  def __init__(self, worker_threads, use_process=False,
               container_executable=None):
    self._worker_threads = worker_threads
    self._use_process = use_process
    self._container_executable = container_executable
    self._worker_processes = {}

  @classmethod
  def start(cls, worker_threads=1, use_process=False, port=0,
            container_executable=None):
    worker_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    worker_address = 'localhost:%s' % worker_server.add_insecure_port(
        '[::]:%s' % port)
    beam_fn_api_pb2_grpc.add_BeamFnExternalWorkerPoolServicer_to_server(
        cls(worker_threads,
            use_process=use_process,
            container_executable=container_executable),
        worker_server)
    worker_server.start()
    return worker_address, worker_server

  def StartWorker(self, start_worker_request, unused_context):
    try:
      if self._use_process:
        command = ['python', '-c',
                   'from apache_beam.runners.worker.sdk_worker '
                   'import SdkHarness; '
                   'SdkHarness("%s",worker_count=%d,worker_id="%s").run()' % (
                       start_worker_request.control_endpoint.url,
                       self._worker_threads,
                       start_worker_request.worker_id)]
        if self._container_executable:
          # command as per container spec
          command = [self._container_executable,
                     '--id=%s' % start_worker_request.worker_id,
                     '--logging_endpoint=%s'
                     % start_worker_request.logging_endpoint.url,
                     '--artifact_endpoint=%s'
                     % start_worker_request.artifact_endpoint.url,
                     '--provision_endpoint=%s'
                     % start_worker_request.provision_endpoint.url,
                     '--control_endpoint=%s'
                     % start_worker_request.control_endpoint.url,
                    ]

        logging.warn("Starting worker with command %s" % command)
        worker_process = subprocess.Popen(command, stdout=subprocess.PIPE)
        self._worker_processes[start_worker_request.worker_id] = worker_process

        # Register to kill the subprocess on exit.
        atexit.register(worker_process.kill)
      else:
        worker = sdk_worker.SdkHarness(
            start_worker_request.control_endpoint.url,
            worker_count=self._worker_threads,
            worker_id=start_worker_request.worker_id)
        worker_thread = threading.Thread(
            name='run_worker_%s' % start_worker_request.worker_id,
            target=worker.run)
        worker_thread.daemon = True
        worker_thread.start()

      return beam_fn_api_pb2.StartWorkerResponse()
    except Exception as exn:
      return beam_fn_api_pb2.StartWorkerResponse(error=str(exn))

  def StopWorker(self, stop_worker_request, unused_context):
    worker_process = self._worker_processes.pop(stop_worker_request.worker_id)
    if worker_process:
      def kill_worker_process():
        try:
          worker_process.kill()
        except OSError:
          # ignore already terminated process
          return
      logging.info("Stopping worker %s" % stop_worker_request.worker_id)
      # communicate is necessary to avoid zombie process
      # time box communicate (it has no timeout parameter in Py2)
      threading.Timer(1, kill_worker_process).start()
      worker_process.communicate()
    return beam_fn_api_pb2.StartWorkerResponse()


def main(argv=None):
  """Entry point for worker pool service for external environments."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--threads_per_worker',
                      type=int,
                      default=12,
                      help='Number of threads per SDK worker.')
  parser.add_argument('--container_executable',
                      type=str,
                      default=None,
                      help='Executable that implements the Beam SDK container contract.')
  parser.add_argument('--service_port',
                      type=int,
                      required=True,
                      help='Bind port for the worker pool service.')

  args, _ = parser.parse_known_args(argv)

  address, server = (BeamFnExternalWorkerPoolServicer.start(
      worker_threads=args.worker_threads,
      use_process=True,
      port=args.servicer_port,
      container_executable=args.container_executable))
  logging.getLogger().setLevel(logging.INFO)
  logging.info('Started worker pool servicer at port: %s with executable: %s',
               address, args.container_executable)
  try:
    while True:
      time.sleep(60 * 60 * 24)
  except KeyboardInterrupt:
    server.stop(0)


if __name__ == '__main__':
  main(sys.argv)
