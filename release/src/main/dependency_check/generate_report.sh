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
# This script will be run by Jenkins as a Python dependency test.

set -e
set -v

PROJECT_ID='apache-beam-testing'
DATASET_ID='beam_dependency_states'
PYTHON_DEP_TABLE_ID='python_dependency_states'
JAVA_DEP_TABLE_ID='java_dependency_states'

# Virtualenv for the rest of the script to run setup
/usr/bin/virtualenv dependency/check
. dependency/check/bin/activate
pip install --upgrade google-cloud-bigquery

rm -f build/dependencyUpdates/beam-dependency-check-report.txt

python release/src/main/dependency_check/generate_dependency_check_report.py \
build/dependencyUpdates/python_dependency_report.txt \
Python \
$PROJECT_ID \
$DATASET_ID \
$PYTHON_DEP_TABLE_ID

python release/src/main/dependency_check/generate_dependency_check_report.py \
build/dependencyUpdates/report.txt \
Java \
$PROJECT_ID \
$DATASET_ID \
$JAVA_DEP_TABLE_ID
