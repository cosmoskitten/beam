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

# This script will be run by Jenkins as a post commit test. In order to run
# locally make the following changes:
#
# LOCAL_PATH   -> Path of tox and virtualenv if you have them already installed.
# GCS_LOCATION -> Temporary location to use for service tests.
# PROJECT      -> Project name to use for dataflow and docker images.
#
# Execute from the root of the repository: sdks/python/container/run_validatescontainer.sh

set -e
set -v

# pip install --user installation location.
LOCAL_PATH=$HOME/.local/bin/

# Where to store integration test outputs.
GCS_LOCATION=gs://temp-storage-for-end-to-end-tests

# Project for the container and integration test
PROJECT=apache-beam-testing

# Verify in the root of the repository
test -d sdks/python/container

# Verify docker and gcloud commands exist
command -v docker
command -v gcloud

# Build the container
DATE=$(date +%Y%m%d-%H%M%S)
CONTAINER=gcr.io/$PROJECT/$USER-$DATE/python
echo "Using container $CONTAINER"
mvn clean install -DskipTests -Pbuild-containers --projects sdks/python/container -amd -Ddocker-repository-root=gcr.io/$PROJECT/$USER-$DATE -amd

# Verify it exists
docker images | grep $CONTAINER

# Push the container
gcloud docker -- push $CONTAINER

# INFRA does not install virtualenv
pip install virtualenv --user

# Virtualenv for the rest of the script to run setup & e2e test
${LOCAL_PATH}/virtualenv sdks/python/container
. sdks/python/container/bin/activate
cd sdks/python
pip install -e .[gcp,test]

# Create a tarball
python setup.py sdist
SDK_LOCATION=$(find dist/apache-beam-*.tar.gz)

# Run ValidatesRunner tests on Google Cloud Dataflow service
echo ">>> RUNNING DATAFLOW RUNNER VALIDATESCONTAINER TEST"
python setup.py nosetests \
  --attr ValidatesContainer \
  --nocapture \
  --processes=1 \
  --process-timeout=900 \
  --test-pipeline-options=" \
    --runner=TestDataflowRunner \
    --project=$PROJECT \
    --worker_harness_container_image=$CONTAINER \
    --staging_location=$GCS_LOCATION/staging-validatesrunner-test \
    --temp_location=$GCS_LOCATION/temp-validatesrunner-test \
    --output=$GCS_LOCATION/output \
    --sdk_location=$SDK_LOCATION \
    --num_workers=1"

# Delete the container locally
docker rmi $CONTAINER

# Delete the container locally and remotely
docker rmi $CONTAINER
gcloud container images delete $CONTAINER --quiet

echo ">>> SUCCESS DATAFLOW RUNNER VALIDATESCONTAINER TEST"
