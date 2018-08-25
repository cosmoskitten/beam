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
jenkinsSyncMetadataTableName = 'jenkins_sync_metadata'

jenkinsJobsSchema = '''
job_name varchar,
build_url varchar,
build_result varchar,
build_timestamp TIMESTAMP,
build_id varchar,
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
timing_waitingDurationMillis integer
'''

# Get the list of jobs.
def getJobs():
  url = 'https://builds.apache.org/view/A-D/view/Beam/api/json?tree=jobs%5Bname,url,lastCompletedBuild%5btimestamp%5d%5D&depth=1'
  r = requests.get(url)
  return r.json()[u'jobs']

def fetchBuildInfo(buildUrl):
  durFields = 'blockedDurationMillis,buildableDurationMillis,buildingDurationMillis,executingTimeMillis,queuingDurationMillis,totalDurationMillis,waitingDurationMillis'
  fields = f'result,timestamp,id,url,builtOn,building,duration,estimatedDuration,fullDisplayName,actions[{durFields}]'
  url = f'{buildUrl}api/json?depth=1&tree={fields}'
  r = requests.get(url)
  return r.json()

def initConnection(host):
  conn = psycopg2.connect(f"dbname='{dbname}' user='{dbusername}' host='{host}' port='{port}' password='{dbpassword}'")
  return conn

def tableExists(connection, tableName):
  cursor = connection.cursor()
  cursor.execute(f"select * from information_schema.tables where table_name='{tableName}';")
  cursor.close()
  return bool(cursor.rowcount)

def createTable(connection, tableName, tableSchema):
  cursor = connection.cursor()
  cmd = f"create table {tableName} ({tableSchema});"
  cursor.execute(cmd)
  cursor.close()
  connection.commit()
  return bool(cursor.rowcount)

def initDbTablesIfNeeded():
  connection  = initConnection(host)

  res = tableExists(connection, jenkinsBuildsTableName)

  if not res:
    res = createTable(connection, jenkinsBuildsTableName, jenkinsJobsSchema)
    if not res:
      raise Exception(f"Failed to create table {jenkinsBuildsTableName}")

  res = tableExists(connection, jenkinsSyncMetadataTableName)

  if not res:
    res = createTable(connection, jenkinsSyncMetadataTableName, 'lastSyncTime timestamp')
    if not res:
      raise Exception(f"Failed to create table {jenkinsSyncMetadataTableName}")

    cur = connection.cursor()
    zeroTime = datetime.fromtimestamp(0)
    cmd = f"insert into {jenkinsSyncMetadataTableName} (lastSyncTime) values (%s)";
    cur.execute(cmd, [zeroTime])
    cur.close()
    connection.commit()

  connection.close()

  return res


# Get all the builds.
def getBuilds(baseUrl):
  durFields = 'blockedDurationMillis,buildableDurationMillis,buildingDurationMillis,executingTimeMillis,queuingDurationMillis,totalDurationMillis,waitingDurationMillis'
  fields = f'result,timestamp,id,url,builtOn,building,duration,estimatedDuration,fullDisplayName,actions[{durFields}]'
  url = f'{baseUrl}api/json?tree=builds[{fields}]&depth=1'
  r = requests.get(url)
  return r.json()[u'builds']

def fetchLastSyncTime(connection):
  cursor = connection.cursor()
  cursor.execute(f"select lastSyncTime from {jenkinsSyncMetadataTableName}")
  timestamp = cursor.fetchone()[0]
  cursor.close()
  return timestamp

def getJobsToFetch(jobs, lowTimestamp, highTimestamp):
  jobs = getJobs()
  supportedJobs = [job for job in jobs if job is not None]
  supportedJobs = [job for job in supportedJobs if job[u'lastCompletedBuild'] is not None]
  supportedJobs = [job for job in supportedJobs if (job[u'lastCompletedBuild'][u'timestamp'] / 1000 > lowTimestamp)]
  supportedJobs = [job for job in supportedJobs if (job[u'lastCompletedBuild'][u'timestamp'] / 1000 < highTimestamp)]
  return supportedJobs

def getBuildsToStore(jobUrl, lowTimestamp, highTimestamp):
  builds = getBuilds(jobUrl)
  builds = [build for build in builds if not build[u'building']]
  builds = [build for build in builds if (build[u'timestamp'] / 1000 > lowTimestamp)]
  builds = [build for build in builds if (build[u'timestamp'] / 1000 < highTimestamp)]
  return builds

def buildRowValuesArray(jobName, build):
  timings = next((x for x in build[u'actions'] if (u'_class' in x) and (x[u'_class'] == u'jenkins.metrics.impl.TimeInQueueAction')), None)
  values = [jobName,
          build[u'url'],
          build[u'result'],
          datetime.fromtimestamp(build[u'timestamp'] / 1000),
          build[u'id'],
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

def updateSyncTimestamp(cursor, newTimestamp):
  cursor.execute(f'delete from {jenkinsSyncMetadataTableName}')
  cursor.execute(f'insert into {jenkinsSyncMetadataTableName} values (%s)', (newTimestamp, ))

def fetchJenkinsRss():
  jenkinsXmlnsTag = '{http://www.w3.org/2005/Atom}'
  url = 'https://builds.apache.org/view/A-D/view/Beam/rssAll'
  r = requests.get(url)
  responseXmlText = r.text
  responseRoot = ElementTree.fromstring(responseXmlText)
  result = []
  for entry in responseRoot.findall(jenkinsXmlnsTag + 'entry'):
    updateTimeStr = entry.find(jenkinsXmlnsTag + 'updated').text
    updateTime = datetime.strptime(updateTimeStr, '%Y-%m-%dT%H:%M:%SZ')
    result.append((updateTime, entry.find(jenkinsXmlnsTag + 'link').get('href')))
  return result

def filterRssBuildsByTime(builds, lowDatetime, highDatetime):
  result = [x for x in builds if x[0] >= lowDatetime]
  result = [x for x in result if x[0] < highDatetime]
  return result

def fetchBacklog():
  maxJobAgeDays = 1

  connection  = initConnection(host)

  lastSyncTime = fetchLastSyncTime(connection)
  lastSyncTimestamp = lastSyncTime.timestamp()

  outdatedThreshold = datetime.now() - timedelta(days=maxJobAgeDays)
  outdatedTimestamp = outdatedThreshold.timestamp()

  lowTime = max(outdatedThreshold, lastSyncTime)
  lowTimestamp = lowTime.timestamp()

  # Fetching a bit outdated data to let Jenkins finish extra processing if any.
  syncTime = datetime.now() - timedelta(minutes=10)
  syncTimestamp = syncTime.timestamp()

  highTime = syncTime
  highTimestamp = syncTimestamp

  jobs = getJobsToFetch(connection, lowTimestamp, highTimestamp)

  cur = connection.cursor()

  for job in jobs:
    print("Fetching builds for", job[u'url'])
    builds = getBuildsToStore(job[u'url'], lowTimestamp, highTimestamp)
    for build in builds:
      rowValues = buildRowValuesArray(job[u'name'], build)
      print(rowValues)
      insertRow(cur, rowValues)

  updateSyncTimestamp(cur, lastSyncTime)

  cur.close()

  connection.commit()

  connection.close()


def extractJobNameFromBuildUrl(url):
  parts = url.split('/')
  return parts[len(parts) - 3]

def fetchNewData():
  connection  = initConnection(host)

  lastSyncTime = fetchLastSyncTime(connection)
  lastSyncTimestamp = lastSyncTime.timestamp()

  lowTime = lastSyncTime
  lowTimestamp = lowTime.timestamp()

  # Fetching a bit outdated data to let Jenkins finish extra processing if any.
  syncTime = datetime.now() - timedelta(minutes=1)
  syncTimestamp = syncTime.timestamp()

  highTime = syncTime
  highTimestamp = syncTimestamp

  rssBuilds = fetchJenkinsRss()
  buildsToFetch = filterRssBuildsByTime(rssBuilds, lowTime, highTime)

  cur = connection.cursor()

  print("Total builds to fetch:", len(buildsToFetch))

  #TODO commit in smaller batches
  i = 0
  for updateTimestamp, buildUrl in buildsToFetch:
    i = i + 1
    print('.', end='')
    if i % 50 == 0:
      print()
      checkmark = i / 50
      print(f'{checkmark}', end='')
    sys.stdout.flush()
    buildInfo = fetchBuildInfo(buildUrl)
    if buildInfo[u'building']:
      print('Job is building', buildInfo[u'url'])
      continue;
    rowValues = buildRowValuesArray(extractJobNameFromBuildUrl(buildInfo[u'url']), buildInfo)
    insertRow(cur, rowValues)

  updateSyncTimestamp(cur, syncTime)

  cur.close()

  connection.commit()

  connection.close()











#################################################################################
print("Started")

print("Checking if DB needs to be initialized")
sys.stdout.flush()
if not initDbTablesIfNeeded():
  raise Exception("Failed to initialize required tables in DB.")


print("Start jobs fetching loop")
sys.stdout.flush()
while True:
  fetchNewData()
  print("Fetched data. Sleeping for 5 min.")
  sys.stdout.flush()
  time.sleep(5*60)

print('Done')


