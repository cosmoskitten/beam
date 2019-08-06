import logging
from google.cloud import pubsub_v1

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.debug("test")

project_id = "ningk-test-project"
topic_name = "hello_topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

logging.info(topic_path)

for n in range(40, 50):
    data = u'Message number {}'.format(n)
    data = data.encode('utf-8')
    future = publisher.publish(topic_path, data=data)
    logging.info('Published {} of message ID {}.'.format(data, future.result()))

logging.info('Published messages.')

import apache_beam as beam
from apache_beam.runners.interactive import interactive_runner
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

pipeline_options = PipelineOptions()
pipeline_options.view_as(StandardOptions).streaming = True
p = beam.Pipeline(interactive_runner.InteractiveRunner(),
                  options=pipeline_options)

messages = p | "NingkRead" >> beam.io.ReadFromPubSub(
            subscription='projects/ningk-test-project/subscriptions/sub_one')

#messages_plus_one = messages | "PlusOne" >> beam.Map(lambda x : x + 'One')

messages | "NingkWrite" >> beam.io.WriteToPubSub(topic_path)
#messages_plus_one | "WritePlusOne" >> beam.io.WriteToPubSub(topic_path)


result = p.run()
result.wait_until_finish()

message_list = result.get(messages)
logging.info(message_list)
#message_plus_one_list = result.get(messages_plus_one)
#logging.info(message_plus_one_list)



