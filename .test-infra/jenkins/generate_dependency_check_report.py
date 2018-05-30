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

# Instantiates a client
bigquery_client = bigquery.Client()

# The name for the new dataset
dataset_id = 'beam_dependency_states'

datasets = list(bigquery_client.list_datasets())
project = bigquery_client.project
print project

if datasets:
    print('Datasets in project {}:'.format(project))
    for dataset in datasets:  # API request(s)
        print('\t{}'.format(dataset.dataset_id))
else:
    print('{} project does not contain any datasets.'.format(project))
