import os
from jira_client import JiraClient
import time

def get_version():
  global_names = {}
  exec(
    open(os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '../../../sdks/python/',
        'apache_beam/version.py')
    ).read(),
    global_names
  )
  return global_names['__version__']

curr_version = get_version()
print curr_version


username = 'BeamJiraBot'
password = 'goBeamgo'
url = 'https://issues.apache.org/jira/'
options = {
  'server': url
}
basic_auth = (username, password)
jira = JiraClient(options, basic_auth, 'BEAM')

summary = 'Test Auto JIRA Subtask 789'
key = 'BEAM-4841'

issue = jira.get_issue_by_key(key)

print issue.fields.resolutiondate
