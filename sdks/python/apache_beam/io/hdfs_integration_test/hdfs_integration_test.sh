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
# Runs Python HDFS integration tests.
#
# Requires docker, docker-compose to be installed.

set -e -u -x

IMAGE_TAG=$(echo hdfs:${BUILD_TAG:-non-jenkins})

# Override DNS settings on GCE to make GCE credentials refresh work.
# GCE credentials are used when reading the test's input file from GCS. This is
# not strictly required since the blob is public, but this solution seemed
# easier.
if hash curl &> /dev/null && \
    curl metadata.google.internal -i -s | \
        grep 'Metadata-Flavor: Google' > /dev/null; then
    DNS_LINE="dns: 169.254.169.254"
    DOCKER_COMPOSE_FILE="docker-compose.yml"
    if ! grep -q ${DNS_LINE} ${DOCKER_COMPOSE_FILE}; then
        echo ${DNS_LINE} >> ${DOCKER_COMPOSE_FILE}
    fi
fi

cd $(dirname $0)
time docker-compose build
time docker-compose up --exit-code-from test --abort-on-container-exit
