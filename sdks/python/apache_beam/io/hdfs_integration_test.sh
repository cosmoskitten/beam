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
# Requires docker to be installed.
# Run from the root directory of the Beam repo.

set -e -u -x

IMAGE_TAG=$(echo hdfs:${BUILD_TAG:-non-jenkins})

# TODO: which gcloud

time docker build . \
    -f sdks/python/apache_beam/io/hdfs_container/Dockerfile \
    --tag $IMAGE_TAG

case ${1:-""} in
    "debug")
        gcloud docker -- run -itP $IMAGE_TAG bash
        ;;
    *)
        time gcloud docker -- run -t $IMAGE_TAG
        ;;
esac
