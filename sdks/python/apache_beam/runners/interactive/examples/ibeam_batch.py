import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.debug("test")

with open('/tmp/abc.txt', 'w+') as f:
    for n in range(1,2):
        f.write(str(n))

# import subprocess
# subprocess.call(['gsutil', 'cp', 'abc.txt', 'gs://ningk-test/'])

import apache_beam as beam
from apache_beam.runners.interactive import interactive_runner
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions()
p = beam.Pipeline(interactive_runner.InteractiveRunner(), options =
                  pipeline_options)

txt = p | "ReadFromFile" >> beam.io.ReadFromText('/tmp/abc.txt')

txt | "WriteToFile" >> beam.io.WriteToText('/tmp/def.txt')


result = p.run()
result.wait_until_finish()

