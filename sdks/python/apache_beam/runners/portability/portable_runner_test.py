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

import inspect
import logging
import platform
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import unittest

import grpc

import apache_beam as beam
from apache_beam import Impulse
from apache_beam import Map
from apache_beam.io.external.kafka import ReadFromKafka
from apache_beam.io.external.kafka import WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import beam_job_api_pb2_grpc
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.portability import fn_api_runner_test
from apache_beam.runners.portability import portable_runner
from apache_beam.runners.portability.local_job_service import LocalJobServicer
from apache_beam.runners.portability.portable_runner import PortableRunner
from apache_beam.runners.worker.channel_factory import GRPCChannelFactory
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.io.external.generate_sequence import GenerateSequence
from apache_beam.metrics import Metrics


class PortableRunnerTest(fn_api_runner_test.FnApiRunnerTest):

  TIMEOUT_SECS = 60

  # Controls job service interaction, not sdk harness interaction.
  _use_subprocesses = False

  def setUp(self):
    if platform.system() != 'Windows':
      def handler(signum, frame):
        msg = 'Timed out after %s seconds.' % self.TIMEOUT_SECS
        print('=' * 20, msg, '=' * 20)
        traceback.print_stack(frame)
        threads_by_id = {th.ident: th for th in threading.enumerate()}
        for thread_id, stack in sys._current_frames().items():
          th = threads_by_id.get(thread_id)
          print()
          print('# Thread:', th or thread_id)
          traceback.print_stack(stack)
        raise BaseException(msg)
      signal.signal(signal.SIGALRM, handler)
      signal.alarm(self.TIMEOUT_SECS)

  def tearDown(self):
    if platform.system() != 'Windows':
      signal.alarm(0)

  @classmethod
  def _pick_unused_port(cls):
    return cls._pick_unused_ports(num_ports=1)[0]

  @staticmethod
  def _pick_unused_ports(num_ports):
    """Not perfect, but we have to provide a port to the subprocess."""
    # TODO(robertwb): Consider letting the subprocess communicate a choice of
    # port back.
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

  @classmethod
  def _start_local_runner_subprocess_job_service(cls):
    cls._maybe_kill_subprocess()
    # TODO(robertwb): Consider letting the subprocess pick one and
    # communicate it back...
    # pylint: disable=unbalanced-tuple-unpacking
    job_port, cls.expansion_port = cls._pick_unused_ports(num_ports=2)
    logging.info('Starting server on port %d.', job_port)
    cls._subprocess = subprocess.Popen(
        cls._subprocess_command(job_port, cls.expansion_port))
    address = 'localhost:%d' % job_port
    job_service = beam_job_api_pb2_grpc.JobServiceStub(
        GRPCChannelFactory.insecure_channel(address))
    logging.info('Waiting for server to be ready...')
    start = time.time()
    timeout = 30
    while True:
      time.sleep(0.1)
      if cls._subprocess.poll() is not None:
        raise RuntimeError(
            'Subprocess terminated unexpectedly with exit code %d.' %
            cls._subprocess.returncode)
      elif time.time() - start > timeout:
        raise RuntimeError(
            'Pipeline timed out waiting for job service subprocess.')
      else:
        try:
          job_service.GetState(
              beam_job_api_pb2.GetJobStateRequest(job_id='[fake]'))
          break
        except grpc.RpcError as exn:
          if exn.code() != grpc.StatusCode.UNAVAILABLE:
            # We were able to contact the service for our fake state request.
            break
    logging.info('Server ready.')
    return address

  @classmethod
  def _get_job_endpoint(cls):
    if '_job_endpoint' not in cls.__dict__:
      cls._job_endpoint = cls._create_job_endpoint()
    return cls._job_endpoint

  @classmethod
  def _create_job_endpoint(cls):
    if cls._use_subprocesses:
      return cls._start_local_runner_subprocess_job_service()
    else:
      cls._servicer = LocalJobServicer()
      return 'localhost:%d' % cls._servicer.start_grpc_server()

  @classmethod
  def get_runner(cls):
    return portable_runner.PortableRunner()

  @classmethod
  def tearDownClass(cls):
    cls._maybe_kill_subprocess()

  @classmethod
  def _maybe_kill_subprocess(cls):
    if hasattr(cls, '_subprocess') and cls._subprocess.poll() is None:
      cls._subprocess.kill()
      time.sleep(0.1)

  def create_options(self):
    def get_pipeline_name():
      for _, _, _, method_name, _, _ in inspect.stack():
        if method_name.find('test') != -1:
          return method_name
      return 'unknown_test'

    # Set the job name for better debugging.
    options = PipelineOptions.from_dictionary({
        'job_name': get_pipeline_name() + '_' + str(time.time())
    })
    options.view_as(PortableOptions).job_endpoint = self._get_job_endpoint()
    # Override the default environment type for testing.
    options.view_as(PortableOptions).environment_type = (
        python_urns.EMBEDDED_PYTHON)
    return options

  def create_pipeline(self):
    return beam.Pipeline(self.get_runner(), self.create_options())

  # Can't read host files from within docker, read a "local" file there.
  def test_read(self):
    with self.create_pipeline() as p:
      lines = p | beam.io.ReadFromText('/etc/profile')
      assert_that(lines, lambda lines: len(lines) > 0)

  def test_no_subtransform_composite(self):
    raise unittest.SkipTest("BEAM-4781")

  def test_external_transforms(self):
    # TODO Move expansion address resides into PipelineOptions
    def get_expansion_service():
      return "localhost:" + str(type(self).expansion_port)

    with self.create_pipeline() as p:
      res = (
          p
          | GenerateSequence(start=1, stop=10,
                             expansion_service=get_expansion_service()))

      assert_that(res, equal_to([i for i in range(1, 10)]))

    # We expect to fail here because we do not have a Kafka cluster handy.
    # Nevertheless, we check that the transform is expanded by the
    # ExpansionService and that the pipeline fails during execution.
    with self.assertRaises(Exception) as ctx:
      with self.create_pipeline() as p:
        # pylint: disable=expression-not-assigned
        (p
         | ReadFromKafka(consumer_config={'bootstrap.servers':
                                            'notvalid1:7777, notvalid2:3531'},
                         topics=['topic1', 'topic2'],
                         key_deserializer='org.apache.kafka.'
                                          'common.serialization.'
                                          'ByteArrayDeserializer',
                         value_deserializer='org.apache.kafka.'
                                            'common.serialization.'
                                            'LongDeserializer',
                         expansion_service=get_expansion_service()))
    self.assertTrue('No resolvable bootstrap urls given in bootstrap.servers'
                    in str(ctx.exception),
                    'Expected to fail due to invalid bootstrap.servers, but '
                    'failed due to:\n%s' % str(ctx.exception))

    # We just test the expansion but do not execute.
    # pylint: disable=expression-not-assigned
    (self.create_pipeline()
     | Impulse()
     | Map(lambda input: (1, input))
     | WriteToKafka(producer_config={'bootstrap.servers':
                                       'localhost:9092, notvalid2:3531'},
                    topic='topic1',
                    key_serializer='org.apache.kafka.'
                                   'common.serialization.'
                                   'LongSerializer',
                    value_serializer='org.apache.kafka.'
                                     'common.serialization.'
                                     'ByteArraySerializer',
                    expansion_service=get_expansion_service()))

  def test_flattened_side_input(self):
    # Blocked on support for transcoding
    # https://jira.apache.org/jira/browse/BEAM-6523
    super(PortableRunnerTest, self).test_flattened_side_input(
        with_transcoding=False)

  def test_metrics(self):
    """Run a simple DoFn that increments a counter, and verify that its
     expected value is written to a temporary file by the FileReporter"""

    counter_name = 'elem_counter'

    class DoFn(beam.DoFn):
      def __init__(self):
        self.counter = Metrics.counter(self.__class__, counter_name)
        logging.info('counter: %s' % self.counter.metric_name)

      def process(self, v):
        self.counter.inc()

    p = self.create_pipeline()
    n = 100

    # pylint: disable=expression-not-assigned
    p \
    | beam.Create(list(range(n))) \
    | beam.ParDo(DoFn())

    result = p.run()
    result.wait_until_finish()

    with open(self.test_metrics_path, 'r') as f:
      lines = [line for line in f.readlines() if counter_name in line]
      self.assertEqual(
          len(lines), 1,
          msg='Expected 1 line matching "%s":\n%s' % (
            counter_name, '\n'.join(lines))
      )
      line = lines[0]
      self.assertTrue(
          '%s: 100' % counter_name in line,
          msg='Failed to find expected counter %s in line %s' % (
            counter_name, line)
      )

  def test_sdf_with_sdf_initiated_checkpointing(self):
    raise unittest.SkipTest("BEAM-2939")

  def test_callbacks_with_exception(self):
    raise unittest.SkipTest("BEAM-6868")

  def test_register_finalizations(self):
    raise unittest.SkipTest("BEAM-6868")

  # Inherits all tests from fn_api_runner_test.FnApiRunnerTest


class PortableRunnerTestWithExternalEnv(PortableRunnerTest):

  @classmethod
  def setUpClass(cls):
    cls._worker_address, cls._worker_server = (
        portable_runner.BeamFnExternalWorkerPoolServicer.start())

  @classmethod
  def tearDownClass(cls):
    cls._worker_server.stop(1)

  def create_options(self):
    options = super(PortableRunnerTestWithExternalEnv, self).create_options()
    options.view_as(PortableOptions).environment_type = 'EXTERNAL'
    options.view_as(PortableOptions).environment_config = self._worker_address
    return options


@unittest.skip("BEAM-3040")
class PortableRunnerTestWithSubprocesses(PortableRunnerTest):
  _use_subprocesses = True

  def create_options(self):
    options = super(PortableRunnerTestWithSubprocesses, self).create_options()
    options.view_as(PortableOptions).environment_type = (
        python_urns.SUBPROCESS_SDK)
    options.view_as(PortableOptions).environment_config = (
        b'%s -m apache_beam.runners.worker.sdk_worker_main' %
        sys.executable.encode('ascii'))
    return options

  @classmethod
  def _subprocess_command(cls, job_port, _):
    return [
        sys.executable,
        '-m', 'apache_beam.runners.portability.local_job_service_main',
        '-p', str(job_port),
    ]


class PortableRunnerInternalTest(unittest.TestCase):
  def test__create_default_environment(self):
    docker_image = PortableRunner.default_docker_image()
    self.assertEqual(
        PortableRunner._create_environment(PipelineOptions.from_dictionary({})),
        beam_runner_api_pb2.Environment(
            urn=common_urns.environments.DOCKER.urn,
            payload=beam_runner_api_pb2.DockerPayload(
                container_image=docker_image
            ).SerializeToString()))

  def test__create_docker_environment(self):
    docker_image = 'py-docker'
    self.assertEqual(
        PortableRunner._create_environment(PipelineOptions.from_dictionary({
            'environment_type': 'DOCKER',
            'environment_config': docker_image,
        })), beam_runner_api_pb2.Environment(
            urn=common_urns.environments.DOCKER.urn,
            payload=beam_runner_api_pb2.DockerPayload(
                container_image=docker_image
            ).SerializeToString()))

  def test__create_process_environment(self):
    self.assertEqual(
        PortableRunner._create_environment(PipelineOptions.from_dictionary({
            'environment_type': "PROCESS",
            'environment_config': '{"os": "linux", "arch": "amd64", '
                                  '"command": "run.sh", '
                                  '"env":{"k1": "v1"} }',
        })), beam_runner_api_pb2.Environment(
            urn=common_urns.environments.PROCESS.urn,
            payload=beam_runner_api_pb2.ProcessPayload(
                os='linux',
                arch='amd64',
                command='run.sh',
                env={'k1': 'v1'},
            ).SerializeToString()))
    self.assertEqual(
        PortableRunner._create_environment(PipelineOptions.from_dictionary({
            'environment_type': 'PROCESS',
            'environment_config': '{"command": "run.sh"}',
        })), beam_runner_api_pb2.Environment(
            urn=common_urns.environments.PROCESS.urn,
            payload=beam_runner_api_pb2.ProcessPayload(
                command='run.sh',
            ).SerializeToString()))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
