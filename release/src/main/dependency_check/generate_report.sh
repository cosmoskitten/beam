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

# Virtualenv for the rest of the script to run setup
/usr/bin/virtualenv dependency/check
. dependency/check/bin/activate
pip install --upgrade google-cloud-bigquery

pwd

python release/src/main/dependency_check/generate_dependency_check_report.py build/dependencyUpdates/python_dependency_report.txt Python
