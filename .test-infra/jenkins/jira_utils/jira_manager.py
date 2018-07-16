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

from jira_client import JiraClient

_JIRA_PROJECT_NAME = 'BEAM'

class JiraManager:

  def __init__(self, jira_url, jira_username, jira_password):
    options = {
      'server': jira_url
    }
    basic_auth = (jira_username, jira_password)
    self.jira = JiraClient(options, basic_auth, _JIRA_PROJECT_NAME)


  def _create_issue(self):
    pass


  def _search_issues(self):
    pass


  def _reopen_issue(self):
    pass


  def _append_descriptions(self):
    pass


  def manage_issues(self):
    pass
