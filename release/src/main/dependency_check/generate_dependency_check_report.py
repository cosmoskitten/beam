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

import sys
import os.path
import re
from google.cloud import bigquery

# # Instantiates a client
# bigquery_client = bigquery.Client()
#
# # The name for the new dataset
# dataset_id = 'beam_dependency_states'
#
# datasets = list(bigquery_client.list_datasets())
# project = bigquery_client.project
# print project
#
# if datasets:
#     print('Datasets in project {}:'.format(project))
#     for dataset in datasets:  # API request(s)
#         print('\t{}'.format(dataset.dataset_id))
# else:
#     print('{} project does not contain any datasets.'.format(project))



def extract_results(file_path):
  outdated_deps = []
  with open(file_path) as raw_report:
    see_oudated_deps = False
    for line in raw_report:
      if see_oudated_deps:
        outdated_deps.append(line)
      if line.startswith('The following dependencies have later release versions:'):
        see_oudated_deps = True
  raw_report.close()
  return outdated_deps

# Extracts and analyze dependency versions and release dates.
# Returns a collection of dependencies which is "high priority":
# 1. dependency has major release. e.g org.assertj:assertj-core [2.5.0 -> 3.10.0]
# 2. dependency is 3 sub-versions behind the newest one. e.g org.tukaani:xz [1.5 -> 1.8]
# 3. TODO: dependency has not been updated for more than 6 months.
def prioritize_dependencies(deps):
  high_priority_deps = []
  for dep in deps:
    if compare_dependency_versions(dep):
      high_priority_deps.append(dep)
    else:
      #TODO: implement BQ tables to resolve release dates
  return high_priority_deps


def compare_dependency_versions(dep):
  curr_ver, latest_ver = extract_versions(dep)
  if curr_ver is None or latest_ver is None:
    return True
  else:
    curr_ver_splitted = curr_ver.split('.')
    latest_ver_splitted = latest_ver.split('.')
    curr_major_ver = curr_ver_splitted[0]
    latest_major_ver = latest_ver_splitted[0]
    # compare major versions
    if curr_major_ver.isdigit() and latest_major_ver.isdigit() and int(curr_major_ver) != int(latest_major_ver):
      return True
    #compare minor versions
    else:
      curr_minor_ver = curr_ver_splitted[1] if len(curr_ver_splitted) > 1 else None
      latest_minor_ver = latest_ver_splitted[1] if len(latest_ver_splitted) > 1 else None
      if curr_minor_ver is not None and latest_minor_ver is not None:
        if not curr_minor_ver.isdigit() or not latest_major_ver.isdigit():
          return True
        elif int(curr_minor_ver) + 3 <= int(latest_major_ver):
          return True
  return False

def extract_versions(dep):
  pattern = "[\s\S]*\[([\s\S]*) -> ([\s\S]*)\]"
  match = re.match(pattern, dep)
  if match is None:
    return None, None
  return match.group(1), match.group(2)


def generate_report(file_path, sdk_type):
  #report_name = 'src/build/dependencyUpdates/beam-dependency-check-report.txt'
  report_name = '../../../../build/dependencyUpdates/beam-dependency-check-report.txt'
  if os.path.exists(report_name):
    append_write = 'a'
  else:
    append_write = 'w'

  # Extract dependency check results from build/dependencyUpdate
  report = open(report_name, append_write)
  if os.path.isfile(file_path):
    outdated_deps = extract_results(file_path)
  else:
    report.write("Did not find the raw report of dependency check: {}".format(file_path))
    report.close()
    return

  # Prioritize dependencies by comparing versions.
  high_priority_deps = prioritize_dependencies(outdated_deps)

  # Write results to a report
  for dep in high_priority_deps:
    report.write("%s" % dep)
  report.close()


def main(args):
  generate_report(args[0], args[1])

if __name__ == '__main__':
  main(sys.argv[1:])
