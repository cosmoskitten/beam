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


set -e
set -v

print_separator() {
    echo "############################################################################"
    echo $1
    echo "############################################################################"
}

update_gcloud() {
    curl https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-189.0.0-linux-x86_64.tar.gz \
    --output gcloud.tar.gz
    tar xf gcloud.tar.gz
    ./google-cloud-sdk/install.sh --quiet
    . ./google-cloud-sdk/path.bash.inc
    gcloud components update --quiet || echo 'gcloud components update failed'
    gcloud -v
}

get_version() {
    # this function will pull python sdk version from sdk/python/apache_beam/version.py and eliminate postfix '.dev'
    version=$(awk '/__version__/{print $3}' sdks/python/apache_beam/version.py)
    echo $version | cut -c 2- | rev | cut -d'.' -f2- | rev
}

get_project_id() {
    echo $PROJECT_ID
}

get_bucket_name() {
    echo $BUCKET_NAME
}

get_temp_dir() {
    echo $TEMP_DIR
}

get_num_workers() {
    echo $NUM_WORKERS
}

get_candidate_url() {
    echo $CANDIDATE_URL
}

get_beam_python_sdk() {
    echo $BEAM_PYTHON_SDK
}

# Configurations for python RC validation
VERSION=2.3.0
CANDIDATE_URL="https://dist.apache.org/repos/dist/dev/beam/$VERSION/"
BEAM_PYTHON_SDK="apache-beam-$VERSION-python.zip"

# Cloud Configurations
PROJECT_ID='apache-beam-testing'
BUCKET_NAME='temp-storage-for-release-validation-tests/nightly-snapshot-validation'
TEMP_DIR='/tmp'
DATASET='beam_postrelease_mobile_gaming'
NUM_WORKERS=1



