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

# Queries Jenkins to collect metrics and pu them in bigquery.
import os
import psycopg2
import re
import requests
import socket
import sys
import time

from datetime import datetime, timedelta
from xml.etree import ElementTree


# list projects           : https://issues.apache.org/jira/rest/api/latest/project
# beam project            : https://issues.apache.org/jira/rest/api/latest/project/BEAM
# test-failures component : https://issues.apache.org/jira/rest/api/latest/component/12334203
# issue search            : https://issues.apache.org/jira/rest/api/latest/search?jql=component=test-failures AND assignee=ardagan

# https://issues.apache.org/jira/rest/api/latest/search?jql=component=test-failures AND status=open AND updated > "0001-01-01 00:01"
# https://issues.apache.org/jira/rest/api/latest/search?jql=component=testing AND updated > "0001-01-01 00:01"&startAt=1690
# https://issues.apache.org/jira/rest/api/latest/search?jql=component=testing AND updated > "0001-01-01 00:01"&startAt=1
# https://issues.apache.org/jira/rest/api/latest/search?jql=component=testing AND updated > "0001-01-01 00:01"&startAt=51&maxResults=50


# https://issues.apache.org/jira/rest/api/latest/search?jql=component=test-failures AND status=open AND updated > "0001-01-01 00:01"&maxResults=50&startAt=0


# What I want for each issue in test-failures:
# id
# link
# status
# creator
# assignee
# updated
# created
# fixed date


# Keeping this as reference for localhost debug
# Fetching docker host machine ip for testing purposes.
# Actual host should be used for production.
# import subprocess
# cmd_out = subprocess.check_output(["ip", "route", "show"]).decode("utf-8")
# host = cmd_out.split(" ")[2]

host = os.environ['JENSYNC_HOST']
port = os.environ['JENSYNC_PORT']
dbname = os.environ['JENSYNC_DBNAME']
dbusername = os.environ['JENSYNC_DBUSERNAME']
dbpassword = os.environ['JENSYNC_DBPWD']

jiraTFIssuesTableName = 'jira_test_failures_issues'

jiraTFIssuesCreateTableQuery = f"""
create table {jiraTFIssuesTableName} (
id varchar NOT NULL,
url varchar,
summary varchar,
status varchar,
creator TIMESTAMP,
assignee varchar,
created TIMESTAMP,
updated TIMESTAMP,
fixed_date TIMESTAMP,
primary key(id)
)
"""


def fetchIssues(lastSyncTime, startAt = 0):
  # time format 0001-01-01 00:01   ==> "yyyy-mm-dd hh:mm"
  url = ('https://issues.apache.org/jira/rest/api/latest/search?'
         'jql=component=test-failures AND status=open '
         'AND updated > "{time}"'
         '&maxResults=50&startAt={startAt}')

  r = requests.get(url)
  return r.json()


def initConnection():
  conn = psycopg2.connect(f"dbname='{dbname}' user='{dbusername}' host='{host}'"
                          f" port='{port}' password='{dbpassword}'")
  return conn


def tableExists(cursor, tableName):
  cursor.execute(f"select * from information_schema.tables"
                 f" where table_name='{tableName}';")
  return bool(cursor.rowcount)


def initDbTablesIfNeeded():
  connection = initConnection()
  cursor = connection.cursor()

  buildsTableExists = tableExists(cursor, jiraTFIssuesTableName)
  print('Builds table exists', buildsTableExists)
  if not buildsTableExists:
    cursor.execute(jiraTFIssuesCreateTableQuery)
    if not bool(cursor.rowcount):
      raise Exception(f"Failed to create table {jiraTFIssuesTableName}")

  cursor.close()
  connection.commit()

  connection.close()


def fetchSyncedJobsBuildVersions(cursor):
  fetchQuery = f'''
  select job_name, max(build_id)
  from {jiraTFIssuesTableName}
  group by job_name
  '''

  cursor.execute(fetchQuery)
  return dict(cursor.fetchall())


def fetchBuildsForJob(jobUrl):
  durFields = ('blockedDurationMillis,buildableDurationMillis,'
               'buildingDurationMillis,executingTimeMillis,queuingDurationMillis,'
               'totalDurationMillis,waitingDurationMillis')
  fields = (f'result,timestamp,id,url,builtOn,building,duration,'
            f'estimatedDuration,fullDisplayName,actions[{durFields}]')
  url = f'{jobUrl}api/json?depth=1&tree=builds[{fields}]'
  r = requests.get(url)
  return r.json()[u'builds']


def buildRowValuesArray(jobName, build):
  timings = next((x
                  for x in build[u'actions']
                  if (u'_class' in x)
                    and (x[u'_class'] == u'jenkins.metrics.impl.TimeInQueueAction')),
                 None)
  values = [jobName,
          int(build[u'id']),
          build[u'url'],
          build[u'result'],
          datetime.fromtimestamp(build[u'timestamp'] / 1000),
          build[u'builtOn'],
          build[u'duration'],
          build[u'estimatedDuration'],
          build[u'fullDisplayName'],
          timings[u'blockedDurationMillis'] if timings is not None else -1,
          timings[u'buildableDurationMillis'] if timings is not None else -1,
          timings[u'buildingDurationMillis'] if timings is not None else -1,
          timings[u'executingTimeMillis'] if timings is not None else -1,
          timings[u'queuingDurationMillis'] if timings is not None else -1,
          timings[u'totalDurationMillis'] if timings is not None else -1,
          timings[u'waitingDurationMillis'] if timings is not None else -1]
  return values


def insertRow(cursor, rowValues):
  cursor.execute(f'insert into {jiraTFIssuesTableName} values (%s, %s, %s, %s,'
                  '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', rowValues)


def fetchNewData():
  connection = initConnection()
  cursor = connection.cursor()
  syncedJobs = fetchSyncedJobsBuildVersions(cursor)
  cursor.close()
  connection.close()

  newJobs = fetchJobs()

  for newJobName, newJobLastBuildId, newJobUrl in newJobs:
    syncedJobId = syncedJobs[newJobName] if newJobName in syncedJobs else -1
    if newJobLastBuildId > syncedJobId:
      builds = fetchBuildsForJob(newJobUrl)
      builds = [x for x in builds if int(x[u'id']) > syncedJobId]

      connection = initConnection()
      cursor = connection.cursor()

      for build in builds:
        if build[u'building']:
          continue;
        rowValues = buildRowValuesArray(newJobName, build)
        print("inserting", newJobName, build[u'id'])
        insertRow(cursor, rowValues)

      cursor.close()
      connection.commit()
      connection.close()  # For some reason .commit() doesn't push data

def probeJenkinsIsUp():
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  result = sock.connect_ex(('builds.apache.org', 443))
  return True if result == 0 else False


################################################################################
if __name__ == '__main__':
  print("Started.")

  print("Checking if DB needs to be initialized.")
  sys.stdout.flush()
  initDbTablesIfNeeded()

  print("Start jobs fetching loop.")
  sys.stdout.flush()

  while True:
    if not probeJenkinsIsUp():
      print("Jenkins is unavailabel, skipping fetching data.")
      continue
    else:
      fetchNewData()
      print("Fetched data.")
    print("Sleeping for 5 min.")
    sys.stdout.flush()
    time.sleep(5 * 60)

  print('Done.')

