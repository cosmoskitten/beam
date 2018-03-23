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
#  This file will verify Apache/Beam release candidate python by following steps:
#
#  1. Download files from RC staging location
#  2. Verify hashes
#  3. Create a new virtualenv and install the SDK
#  4. Run Wordcount examples with DirectRunner
#  5. Run Wordcount examples with DataflowRunner
#  6. Run streaming wordcount on DirectRunner
#  7. Run streaming wordcount on DataflowRunner
#

./release/src/main/python-release/run_release_candidate_python_quickstart.sh
./release/src/main/python-release/run_release_candidate_python_moibile_gaming.sh



