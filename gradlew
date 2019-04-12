#!/usr/bin/env bash

################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

echo "Provided arguments:" "$@"

args=("$@")

declare -a mappings

# Load project-mappings file into mappings.
let i=0
while IFS=$'\n' read -r line_data; do
    mappings[i]="${line_data}"
    ((++i))
done < project-mappings

for i in "${!args[@]}"
do
  for mapping in "${mappings[@]}"
  do
     map=($mapping)
     args[$i]=${args[$i]/${map[0]}/${map[1]}}
  done
done

echo "Passing to gradle script:"  "${args[@]}"

./gradlew_orig "${args[@]}"