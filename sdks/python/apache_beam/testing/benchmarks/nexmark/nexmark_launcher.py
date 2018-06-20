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

"""Nexmark launcher.

The Nexmark suite is a series of queries (streaming pipelines) performed
on a simulation of auction events. The launcher orchestrates the generation
and parsing of streaming events and the running of queries.

Model
- Person: Author of an auction or a bid.
- Auction: Item under auction.
- Bid: A bid for an item under auction.

Events
 - Create Person
 - Create Auction
 - Create Bid

Queries
- Query0: Pass through (send and receive auction events).

Usage
- DirectRunner
python nexmark_launcher.py \
    --query/q <query number> \
    --project <project id> \
    --streaming \

- DataflowRunner
python nexmark_launcher.py \
    --query/q <query number> \
    --project <project id> \
    --streaming \
    --sdk_location <apache_beam tar.gz> \
    --staging_location=gs://... \
    --temp_location=gs://

"""
from __future__ import print_function

import argparse
import logging
import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TestOptions

from nexmark_util import Command
from queries import query0

logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)-10s) %(message)s')

# Reserved configuration to run these benchmark suite.
PUBSUB_PROJECT = 'google.com:clouddfe'
PUBSUB_SUBSCRIPTION = 'nexmark-benchmark'
PUBSUB_TOPIC = 'nexmark-benchmark'

# TODO(mariagh): Make comprehensive and large
EVENT_COUNT_PER_TYPE = 3
PERSON_EVENT = 'p12345,maria,maria@maria.com,1234-5678-9012-3456,sunnyvale,CA,1528098831536' # pylint: disable=line-too-long
AUCTION_EVENT = 'a12345,car,2012 hyundai elantra,20K,15K,1528098831536,20180630,maria,vehicle' # pylint: disable=line-too-long
BID_EVENT = 'b12345,maria,354,1528098831536'


def parse_args():
  parser = argparse.ArgumentParser()

  parser.add_argument('--query', '-q',
                      type=int,
                      action='append',
                      required=True,
                      choices=[0, 1],
                      help='Query to run')

  parser.add_argument('--subscription',
                      type=str,
                      help='Pub/Sub subscription to read from')

  parser.add_argument('--topic',
                      type=str,
                      help='Pub/Sub topic to read from')

  args, pipeline_args = parser.parse_known_args()
  options = PipelineOptions(pipeline_args)
  print('args, pipeline_args:', args, pipeline_args)

  # Usage with Dataflow requires a project to be supplied.
  if options.view_as(GoogleCloudOptions).project is None:
    parser.print_usage()
    print(sys.argv[0] + ': error: argument --project is required')
    sys.exit(1)

  # Pub/Sub is currently available for use only in streaming pipelines.
  if options.view_as(StandardOptions).streaming is None:
    parser.print_usage()
    print(sys.argv[0] + ': error: argument --streaming is required')
    sys.exit(1)

  # wait_until_finish ensures that the streaming job is canceled.
  if options.view_as(TestOptions).wait_until_finish_duration is None:
    parser.print_usage()
    print(sys.argv[0] + ': error: argument --wait_until_finish_duration is required') # pylint: disable=line-too-long
    sys.exit(1)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  options.view_as(SetupOptions).save_main_session = True
  return args, options


def generate_events(pipeline, args, pipeline_options):
  from google.cloud import pubsub
  publish_client = pubsub.Client(project=PUBSUB_PROJECT)
  topic = publish_client.topic(PUBSUB_TOPIC)
  sub = topic.subscription(PUBSUB_SUBSCRIPTION)
  assert topic.exists()
  assert sub.exists()

  logging.info('Generating %d auction events to topic %s',
               EVENT_COUNT_PER_TYPE*3, topic.name)
  for _ in xrange(EVENT_COUNT_PER_TYPE):
    topic.publish(PERSON_EVENT)
    topic.publish(AUCTION_EVENT)
    topic.publish(BID_EVENT)

  logging.info('Finished event generation.')

  # Read from PubSub into a PCollection.
  if args.subscription:
    raw_events = pipeline | 'ReadPubSub' >> beam.io.ReadFromPubSub(
        subscription=args.subscription)
  else:
    raw_events = pipeline | 'ReadPubSub' >> beam.io.ReadFromPubSub(
        topic=args.topic)

  return raw_events


def run_query(query, args, pipeline_options):
  try:
    pipeline = beam.Pipeline(options=pipeline_options)
    raw_events = generate_events(pipeline, args, pipeline_options)
    query.load(raw_events)
    result = pipeline.run()
    job_duration = pipeline_options.view_as(TestOptions).wait_until_finish_duration # pylint: disable=line-too-long
    if pipeline_options.view_as(StandardOptions).runner == 'DataflowRunner':
      result.wait_until_finish(duration=job_duration)
      result.cancel()
    else:
      result.wait_until_finish()
  except NotImplementedError as exp:
    print(exp)


def run(argv=None):
  args, _ = parse_args()

  queries = {
      0: query0,
      # TODO(mariagh): Add more queries
  }

  for i in args.query:
    args, pipeline_options = parse_args()
    logging.info('Running query %d', i)

    job_setup_time = 20 * 1000    # in milliseconds
    query_duration = pipeline_options.view_as(TestOptions).wait_until_finish_duration + job_setup_time # pylint: disable=line-too-long
    command = Command(run_query, args=[queries[i], args, pipeline_options])
    try:
      command.run(timeout=query_duration/1000)
    except Exception as exp:
      logging.error('Command failed with "%s"', exp)

  logging.info('Queries run successfully: %s', args.query)


if __name__ == '__main__':
  run()
