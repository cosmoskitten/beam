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
import os
import subprocess
import threading
import time
import traceback
import uuid
from builtins import object
from concurrent import futures

import grpc
from google.protobuf import text_format

from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_job_api_pb2_grpc
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.portability import fn_api_runner

TERMINAL_STATES = [
    beam_job_api_pb2.JobState.DONE,
    beam_job_api_pb2.JobState.STOPPED,
    beam_job_api_pb2.JobState.FAILED,
    beam_job_api_pb2.JobState.CANCELLED,
]


class LocalJobServicer(beam_job_api_pb2_grpc.JobServiceServicer):
  """
    Experimental: No backward compatibility guaranteed.
    Servicer for the Beam Job API.

    Manages one or more pipelines, possibly concurrently.

    This JobService uses a basic local implementation of runner to run the job.
    This JobService is not capable of managing job on remote clusters.

    By default, this JobService executes the job in process but still uses GRPC
    to communicate pipeline and worker state.  It can also be configured to use
    inline calls rather than GRPC (for speed) or launch completely separate
    subprocesses for the runner and worker(s).
    """

  def __init__(self):
    self._jobs = {}

  def start_grpc_server(self, port=0):
    self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    port = self._server.add_insecure_port('localhost:%d' % port)
    beam_job_api_pb2_grpc.add_JobServiceServicer_to_server(self, self._server)
    self._server.start()
    logging.info('Grpc server started on port %s', port)
    return port

  def Prepare(self, request, context=None):
    # For now, just use the job name as the job id.
    logging.debug('Got Prepare request.')
    preparation_id = '%s-%s' % (request.job_name, uuid.uuid4())
    self._jobs[preparation_id] = BeamJob(request.pipeline)
    logging.debug("Prepared job '%s' as '%s'", request.job_name, preparation_id)
    # TODO(angoenka): Pass an appropriate staging_session_token. The token can
    # be obtained in PutArtifactResponse from JobService
    return beam_job_api_pb2.PrepareJobResponse(
        preparation_id=preparation_id, staging_session_token='token')

  def Run(self, request, context=None):
    job_id = request.preparation_id
    logging.info("Runing job '%s'", job_id)
    self._jobs[job_id].start()
    return beam_job_api_pb2.RunJobResponse(job_id=job_id)

  def GetState(self, request, context=None):
    return beam_job_api_pb2.GetJobStateResponse(
        state=self._jobs[request.job_id].state)

  def Cancel(self, request, context=None):
    self._jobs[request.job_id].cancel()
    return beam_job_api_pb2.CancelJobRequest(
        state=self._jobs[request.job_id].state)

  def GetStateStream(self, request, context=None):
    job = self._jobs[request.job_id]
    for state in job.GetStateStream():
      yield state

  def GetMessageStream(self, request, context=None):
    job = self._jobs[request.job_id]
    for log in job.GetMessageStream():
      yield log


class SubprocessSdkWorker(object):
  """Manages a SDK worker implemented as a subprocess communicating over grpc.
    """

  def __init__(self, worker_command_line, control_address):
    self._worker_command_line = worker_command_line
    self._control_address = control_address

  def run(self):
    logging_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    logging_port = logging_server.add_insecure_port('[::]:0')
    logging_server.start()
    logging_servicer = BeamFnLoggingServicer()
    beam_fn_api_pb2_grpc.add_BeamFnLoggingServicer_to_server(
        logging_servicer, logging_server)
    logging_descriptor = text_format.MessageToString(
        endpoints_pb2.ApiServiceDescriptor(url='localhost:%s' % logging_port))

    control_descriptor = text_format.MessageToString(
        endpoints_pb2.ApiServiceDescriptor(url=self._control_address))

    p = subprocess.Popen(
        self._worker_command_line,
        shell=True,
        env=dict(
            os.environ,
            CONTROL_API_SERVICE_DESCRIPTOR=control_descriptor,
            LOGGING_API_SERVICE_DESCRIPTOR=logging_descriptor))
    try:
      p.wait()
      if p.returncode:
        raise RuntimeError(
            'Worker subprocess exited with return code %s' % p.returncode)
    finally:
      if p.poll() is None:
        p.kill()
      logging_server.stop(0)


class BeamJob(threading.Thread):
  """This class handles running and managing a single pipeline.

    The current state of the pipeline is available as self.state.
    """

  def __init__(self, pipeline_proto):
    super(BeamJob, self).__init__()
    self._pipeline_proto = pipeline_proto
    self._state = None
    self._logs = []
    self._final_log_count = -1
    self._state_changes = []
    self._final_state_count = -1
    self._state_notifier = threading.Condition()
    self._log_notifier = threading.Condition()
    self.state = beam_job_api_pb2.JobState.STARTING

  @property
  def state(self):
    return self._state

  @state.setter
  def state(self, new_state):
    """Sets the job state.

    This will inform GetStateStream and GetMessageStream of the new state.
    """

    self._state_notifier.acquire()
    self._log_notifier.acquire()

    self._state_changes.append(
        beam_job_api_pb2.GetJobStateResponse(state=new_state))
    self._logs.append(
        beam_job_api_pb2.JobMessagesResponse(
            state_response=beam_job_api_pb2.GetJobStateResponse(
                state=new_state)))
    self._state = new_state

    self._log_notifier.notify_all()
    self._state_notifier.notify_all()

    self._log_notifier.release()
    self._state_notifier.release()

  def write_log(self, log):
    self._log_notifier.acquire()
    self._logs.append(log)
    self._log_notifier.notify_all()
    self._log_notifier.release()

  def _cleanup(self):
    self._state_notifier.acquire()
    self._log_notifier.acquire()

    self._final_log_count = len(self._logs)
    self._final_state_count = len(self._state_changes)

    self._state_notifier.notify_all()
    self._log_notifier.notify_all()

    self._log_notifier.release()
    self._state_notifier.release()

  def run(self):
    with JobLogHandler(self):
      try:
        fn_api_runner.FnApiRunner().run_via_runner_api(self._pipeline_proto)
        logging.info('Successfully completed job.')
        self.state = beam_job_api_pb2.JobState.DONE
      except:  # pylint: disable=bare-except
        logging.exception('Error running pipeline.')
        logging.exception(traceback.format_exc())
        self.state = beam_job_api_pb2.JobState.FAILED
        raise
      finally:
        # In order for consumers to read all messages, this must be the final
        # instruction after a terminal state.
        self._cleanup()

  def cancel(self):
    if self.state not in TERMINAL_STATES:
      self.state = beam_job_api_pb2.JobState.CANCELLING
      # TODO(robertwb): Actually cancel...
      self.state = beam_job_api_pb2.JobState.CANCELLED
      self._cleanup()

  def _stream_array(self, array, notifier, final_count):
    """Yields all elements in array until array length reaches final_count.

    This method streams all elements in array. It uses the notifier to wait
    until new messages are received. The streams ends when this method emits
    up to final_count number of elements.
    """
    index = 0

    # Pull all  changes until the job finishes.
    while index != self.__dict__[final_count]:
      notifier.acquire()
      notifier.wait(5)
      while index < len(array):
        yield array[index]
        index += 1
      notifier.release()

  def GetStateStream(self):
    """Yields all past and future states.

    This method guarentees that the consumer will see all job state transitions.
    """

    # Pull all state changes until the job finishes.
    for state in self._stream_array(
        array=self._state_changes,
        notifier=self._state_notifier,
        final_count='_final_state_count'):
      yield state

  def GetMessageStream(self):
    """Yields all past and future messages.

    This method guarentees that the consumer will see all messages the job
    generates until it terminates.
    """

    # Subscribers start with the first message and incrementally yield
    # subsequent logs. This process repeats until the job terminates and we know
    # the final amount of logs generated.
    for log in self._stream_array(
        array=self._logs,
        notifier=self._log_notifier,
        final_count='_final_log_count'):
      yield log


class BeamFnLoggingServicer(beam_fn_api_pb2_grpc.BeamFnLoggingServicer):

  def Logging(self, log_bundles, context=None):
    for log_bundle in log_bundles:
      for log_entry in log_bundle.log_entries:
        logging.info('Worker: %s', str(log_entry).replace('\n', ' '))
    return iter([])


class JobLogHandler(logging.Handler):
  """Captures logs to be returned via the Beam Job API.

    Enabled via the 'with' statement."""

  # Mapping from logging levels to LogEntry levels.
  LOG_LEVEL_MAP = {
      logging.FATAL: beam_job_api_pb2.JobMessage.JOB_MESSAGE_ERROR,
      logging.CRITICAL: beam_job_api_pb2.JobMessage.JOB_MESSAGE_ERROR,
      logging.ERROR: beam_job_api_pb2.JobMessage.JOB_MESSAGE_ERROR,
      logging.WARNING: beam_job_api_pb2.JobMessage.JOB_MESSAGE_WARNING,
      logging.INFO: beam_job_api_pb2.JobMessage.JOB_MESSAGE_BASIC,
      logging.DEBUG: beam_job_api_pb2.JobMessage.JOB_MESSAGE_DEBUG,
  }

  def __init__(self, job):
    super(JobLogHandler, self).__init__()
    self._last_id = 0
    self._logged_thread = None
    self._job = job

  def __enter__(self):
    # Remember the current thread to demultiplex the logs of concurrently
    # running pipelines (as Python log handlers are global).
    self._logged_thread = threading.current_thread()
    logging.getLogger().addHandler(self)

  def __exit__(self, *args):
    self._logged_thread = None
    self.close()

  def _next_id(self):
    self._last_id += 1
    return str(self._last_id)

  def emit(self, record):
    if self._logged_thread is threading.current_thread():
      msg = beam_job_api_pb2.JobMessagesResponse(
          message_response=beam_job_api_pb2.JobMessage(
              message_id=self._next_id(),
              time=time.strftime('%Y-%m-%d %H:%M:%S.',
                                 time.localtime(record.created)),
              importance=self.LOG_LEVEL_MAP[record.levelno],
              message_text=self.format(record)))
      self._job.write_log(msg)
