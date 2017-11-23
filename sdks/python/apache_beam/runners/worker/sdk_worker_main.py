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
"""SDK Fn Harness entry point."""
import BaseHTTPServer
import logging
import os
import sys
import threading
import traceback

from google.protobuf import text_format

from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.worker.log_handler import FnApiLogRecordHandler
from apache_beam.runners.worker.sdk_worker import SdkHarness

# This module is experimental. No backwards-compatibility guarantees.


class StatusServer(object):

  def start(self, STATUS_HTTP_PORT=0, started_callback=None):
    """Executes the serving loop for the status server."""

    class StatusHttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):
      """HTTP handler for serving stacktraces of all worker threads."""

      def do_GET(self):  # pylint: disable=invalid-name
        """Return /threadz information for any GET request."""
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        frames = sys._current_frames()  # pylint: disable=protected-access
        for t in threading.enumerate():
          self.wfile.write('--- Thread #%s name: %s ---\n' % (t.ident, t.name))
          self.wfile.write(''.join(traceback.format_stack(frames[t.ident])))

      def log_message(self, f, *args):
        """Do not log any messages."""
        pass

    self.httpd = httpd = BaseHTTPServer.HTTPServer(
        ('localhost', STATUS_HTTP_PORT), StatusHttpHandler)
    logging.info('Status HTTP server running at %s:%s', httpd.server_name,
                 httpd.server_port)

    if started_callback:
      started_callback()

    httpd.serve_forever()


def main(unused_argv):
  """Main entry point for SDK Fn Harness."""
  if 'LOGGING_API_SERVICE_DESCRIPTOR' in os.environ:
    logging_service_descriptor = endpoints_pb2.ApiServiceDescriptor()
    text_format.Merge(os.environ['LOGGING_API_SERVICE_DESCRIPTOR'],
                      logging_service_descriptor)

    # Send all logs to the runner.
    fn_log_handler = FnApiLogRecordHandler(logging_service_descriptor)
    # TODO(vikasrk): This should be picked up from pipeline options.
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(fn_log_handler)
  else:
    fn_log_handler = None

  # Start status HTTP server thread.
  thread = threading.Thread(target=StatusServer().start)
  thread.daemon = True
  thread.setName('status-server-demon')
  thread.start()

  try:
    logging.info('Python sdk harness started.')
    service_descriptor = endpoints_pb2.ApiServiceDescriptor()
    text_format.Merge(os.environ['CONTROL_API_SERVICE_DESCRIPTOR'],
                      service_descriptor)
    # TODO(robertwb): Support credentials.
    assert not service_descriptor.oauth2_client_credentials_grant.url
    SdkHarness(service_descriptor.url).run()
    logging.info('Python sdk harness exiting.')
  except:  # pylint: disable=broad-except
    logging.exception('Python sdk harness failed: ')
    raise
  finally:
    if fn_log_handler:
      fn_log_handler.close()


if __name__ == '__main__':
  main(sys.argv)
