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
"""Entry point for launching the worker pool service for external environment."""

from __future__ import absolute_import

import argparse
import logging
import sys
import time

from apache_beam.runners.portability import portable_runner

def main(argv=None):
    """Entry point for launching the worker pool service for external environment."""

    parser = argparse.ArgumentParser()
    parser.add_argument('--worker_threads',
                        type=int,
                        default=12,
                        help='Number of threads per SDK worker.')
    parser.add_argument('--container_executable',
                        type=str,
                        default=None,
                        help='Executable that implements the container contract.')
    parser.add_argument('--servicer_port',
                        type=int,
                        required=True,
                        help='Bind port for the worker pool service.')

    args, _unknown_args = parser.parse_known_args(argv)

    address, server = (portable_runner.BeamFnExternalWorkerPoolServicer.start(
        worker_threads=args.worker_threads,
        use_process=True,
        port=args.servicer_port,
        container_executable=args.container_executable))
    logging.getLogger().setLevel(logging.INFO)
    logging.info('Started worker pool servicer at port: %s with executable: %s', address, args.container_executable)
    try:
        while True:
            time.sleep(60 * 60 * 24)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    main(sys.argv)
