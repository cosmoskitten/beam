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
import time
import requests
import psycopg2
import os
import re
from datetime import datetime, timedelta
import sys
from xml.etree import ElementTree


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

jenkinsBuildsTableName = 'jenkins_builds'

jenkinsJobsCreateTableQuery = f"""
create table {jenkinsBuildsTableName} (
job_name varchar NOT NULL,
build_id integer NOT NULL,
build_url varchar,
build_result varchar,
build_timestamp TIMESTAMP,
build_builtOn varchar,
build_duration integer,
build_estimatedDuration integer,
build_fullDisplayName varchar,
timing_blockedDurationMillis integer,
timing_buildableDurationMillis integer,
timing_buildingDurationMillis integer,
timing_executingTimeMillis integer,
timing_queuingDurationMillis integer,
timing_totalDurationMillis integer,
timing_waitingDurationMillis integer,
primary key(job_name, build_id)
)
"""

# returns (jobName, lastBuildId, jobUrl)
def fetchJobs():
  url = 'https://builds.apache.org/view/A-D/view/Beam/api/json?tree=jobs[name,url,lastCompletedBuild[id]]&depth=1'
  r = requests.get(url)
  jobs = r.json()[u'jobs']
  print("---")
  print(jobs)
  result = map(lambda x: (x['name'], int(x['lastCompletedBuild']['id']) if x['lastCompletedBuild'] is not None else -1, x['url']), jobs)
  return result

def initConnection():
  conn = psycopg2.connect(f"dbname='{dbname}' user='{dbusername}' host='{host}' port='{port}' password='{dbpassword}'")
  return conn

def tableExists(cursor, tableName):
  cursor.execute(f"select * from information_schema.tables where table_name='{tableName}';")
  return bool(cursor.rowcount)

def createTable(connection, tableName, tableSchema):
  cursor = connection.cursor()
  cmd = f"create table {tableName} ({tableSchema});"
  cursor.execute(cmd)
  cursor.close()
  connection.commit()
  return bool(cursor.rowcount)


def initDbTablesIfNeeded():
  connection = initConnection()
  cursor = connection.cursor()

  res = tableExists(cursor, jenkinsBuildsTableName)
  if not res:
    cursor.execute(jenkinsJobsCreateTableQuery)
    if not bool(cursor.rowcount):
      raise Exception(f"Failed to create table {jenkinsBuildsTableName}")

  cursor.close()
  connection.commit()

  connection.close()

def fetchSyncedJobsBuildVersions(cursor):
  fetchQuery = f'''
  select job_name, max(build_id)
  from {jenkinsBuildsTableName}
  group by job_name
  '''

  cursor.execute(fetchQuery)
  return dict(cursor.fetchall())

def fetchBuildsForJob(jobUrl):
  durFields = 'blockedDurationMillis,buildableDurationMillis,buildingDurationMillis,executingTimeMillis,queuingDurationMillis,totalDurationMillis,waitingDurationMillis'
  fields = f'result,timestamp,id,url,builtOn,building,duration,estimatedDuration,fullDisplayName,actions[{durFields}]'
  url = f'{jobUrl}api/json?depth=1&tree=builds[{fields}]'
  r = requests.get(url)
  return r.json()[u'builds']

def buildRowValuesArray(jobName, build):
  timings = next((x for x in build[u'actions'] if (u'_class' in x) and (x[u'_class'] == u'jenkins.metrics.impl.TimeInQueueAction')), None)
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
  cursor.execute(f'insert into {jenkinsBuildsTableName} values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', rowValues)


def fetchNewData():
  connection = initConnection()
  cursor = connection.cursor()
  syncedJobs = fetchSyncedJobsBuildVersions(cursor)

  newJobs = fetchJobs()

  for newJobName, newJobLastBuildId, newJobUrl in newJobs:
    syncedJobId = syncedJobs[newJobName] if newJobName in syncedJobs else -1
    print(syncedJobId, newJobLastBuildId)
    if newJobLastBuildId > syncedJobId:
      print(newJobName, newJobLastBuildId, newJobUrl)
      builds = fetchBuildsForJob(newJobUrl)
      builds = [x for x in builds if int(x[u'id']) > syncedJobId]
      for build in builds:
        rowValues = buildRowValuesArray(newJobName, build)
        print("inserting", newJobName, build[u'id'])
        insertRow(cursor, rowValuest)
      cursor.close()
      connection.commit()
      connection.close()
      connection = initConnection()
      cursor = connection.cursor()

  cursor.close()
  connection.close()

#################################################################################
print("Started.")

print("Checking if DB needs to be initialized.")
sys.stdout.flush()
initDbTablesIfNeeded()

print("Start jobs fetching loop.")
sys.stdout.flush()

while True:
  fetchNewData()
  print("Fetched data. Sleeping for 5 min.")
  sys.stdout.flush()
  time.sleep(5 * 60)

print('Done.')



