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

from google.cloud import bigquery


class BigQueryClientUtils:

  def __init__(self, project_id):
    self.bigquery_client = bigquery.Client(project_id)
    self.dataset_ref = self.bigquery_client.dataset("beam_dependency_states")
    self.table_ref = self.dataset_ref.table("experiment_dependency_check")


  def query_dep_release_date(self, dep, version):
    """
    Query for release date of a specific version
    Args:
      dep, version
    Return:
      release_date
    """
    # TODO: implementation
    return None

  def update_dep_to_table(self, dep, version, release_date):
    """
    Update a dependency with version and release date into bigquery table
    Args:
      dep, version
    Return:
      release_date
    """

  def cleanup_old_states(self):
    """
    Remove old states which is not currently used and more than 12 month from now
    """
    print "clean up!"

