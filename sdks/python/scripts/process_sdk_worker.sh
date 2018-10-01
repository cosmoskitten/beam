#!/bin/bash
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

###########################################################################
# This script will be run the python sdk worker.
#
# Positional Parameters:
# --id=<WorkerId>
# --logging_endpoint=<host:port>
# --artifact_endpoint=<host:port>
# --provision_endpoint=<host:port>
# --control_endpoint=<host:port
#
# It assumes environment has pre-installed Beam and python worker code.
# Note: No artifact staging is done. User is responsible of setting up
# environment correctly including the user code.

# Example parameters while execution --id=1 --logging_endpoint=localhost:46213 --artifact_endpoint=localhost:34075 --provision_endpoint=localhost:36555 --control_endpoint=localhost:33339

export WORKER_ID=`echo $1 | cut -d'=' -f 2`
export CONTROL_API_SERVICE_DESCRIPTOR="url: \"`echo $5 | cut -d'=' -f 2`\""
export LOGGING_API_SERVICE_DESCRIPTOR="url: \"`echo $2 | cut -d'=' -f 2`\""

echo "Starting harness $WORKER_ID with arguments $@"
# Launch actual SDK Harness
container/build/target/launcher/linux_amd64/boot $@
echo "Harness $WORKER_ID Shutdown"
