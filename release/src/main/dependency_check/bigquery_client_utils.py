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

  def __init__(self, project_id, dataset_id, table_id):
    self.project_id = project_id
    self.dataset_id = dataset_id
    self.table_id = table_id
    self.bigquery_client = bigquery.Client(project_id)
    self.table_ref = self.bigquery_client.dataset(dataset_id).table(table_id)
    self.table = self.bigquery_client.get_table(self.table_ref)


  def query_dep_release_date(self, dep, version):
    """
    Query for release date of a specific version
    Args:
      dep, version
    Return:
      release_date
    """
    query = """SELECT release_date FROM `{0}.{1}.{2}` 
    WHERE package_name=\'{3}\' and version=\'{4}\'""".format(self.project_id, self.dataset_id, self.table_id, dep.strip(), version.strip())

    query_job = self.bigquery_client.query(query)
    rows = list(query_job)
    if len(rows) == 0:
      return None
    assert len(rows) == 1
    return rows[0]['release_date']


  def add_dep_to_table(self, dep, version, release_date, is_current_using=False):
    """
    Add a dependency with version and release date into bigquery table
    Args:
      dep, version, is_current_using (default False)
    Return:
      release_date
    """
    dep_to_insert = [
      (dep, version, release_date, is_current_using)
    ]
    print "adding {} to BQ table".format(dep_to_insert[0])
    err = self.bigquery_client.insert_rows(self.table, dep_to_insert)
    if err is not None:
      print "Error: {}".format(err)


  def remove_dep_from_table(self, dep, version, is_current_using=False):
    """
    Remove a dependency from bigquery table
    Args:
      dep, is_current_using (default False)
    """


  def cleanup_old_states(self):
    """
    Remove old states which is not currently used and more than 12 month from now
    """
    print "clean up!"

