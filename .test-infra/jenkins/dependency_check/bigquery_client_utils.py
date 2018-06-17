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
from datetime

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
    query = """SELECT release_date 
      FROM `{0}.{1}.{2}` 
      WHERE package_name=\'{3}\' AND version=\'{4}\'""".format(self.project_id,
                                                               self.dataset_id,
                                                               self.table_id,
                                                               dep.strip(),
                                                               version.strip())

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
    """
    dep_to_insert = [
      (dep, version, release_date, is_current_using)
    ]
    err = self.bigquery_client.insert_rows(self.table, dep_to_insert)
    if len(err) != 0:
      print "Found errors when inserting {1} to BQ table {2}:\n {3}".format(dep_to_insert[0], self.table_id, err)


  def update_dep_to_table(self, dep, version, is_current_using):
    """
    Update a dependency states.
    Args:
      dep, version, is_current_using
    """
    query = """UPDATE `{0}.{1}.{2}`
    SET is_current_using = \'{3}\'
    WHERE package_name=\'{4}\' AND version=\'{5}\'""".format(self.project_id,
                                                             self.dataset_id,
                                                             self.table_id,
                                                             is_current_using,
                                                             dep.strip(),
                                                             version.strip())
    self.bigquery_client.query(query)


  def clean_stale_records_from_table(self):
    """
    Remove stale records from the table. A record is stale if it is not currently used and the release date is behind 3
    years.
    """
    query = """SELECT *  
      FROM `{0}.{1}.{2}` 
      WHERE DATE(release_date) < {3}""".format(self.project_id,
                                                self.dataset_id,
                                                self.table_id,
                                                datetime.datetime.today().date() - datetime.timedelta(3*365))
    self.bigquery_client.query(query)