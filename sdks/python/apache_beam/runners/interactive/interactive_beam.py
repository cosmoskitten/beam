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

"""Module of iBeam (interactive Beam) features.

The purpose of the module is to reduce the learning curve of iBeam users,
provide a single place for importing and add sugar syntax for all iBeam
components. It gives users capability to manipulate existing environment for
interactive beam, create pipeline with interactive runner, run interactive
pipeline on selected runner as normal pipeline and visualize PCollections as
bounded dataset.

Note: iBeam works the same as normal Beam with DirectRunner when not in an
interactive environment such as Jupyter lab or Jupyter Notebook. You can also
run pipeline created by iBeam as normal Beam pipeline by run_pipeline() with
desired runners.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners.dataflow import dataflow_runner
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner


def watch(watchable):
  """Watches a watchable so that iBeam can understand your pipeline.

  If you write Beam pipeline in a notebook or __main__ module directly, since
  __main__ module is always watched by default, you don't have to instruct
  iBeam. However, if your Beam pipeline is defined in some module other than
  __main__, e.g., inside a class function or a unit test, you can watch() the
  scope to instruct iBeam to apply magic to your pipeline when running pipeline
  interactively.

    For example:

    class Foo(object)
      def build_pipeline(self):
        p = create_pipeline()
        init_pcoll = p |  'Init Create' >> beam.Create(range(10))
        watch(locals())
        return p
    Foo().build_pipeline().run()

    iBeam will cache init_pcoll for the first run. You can use:

    visualize(init_pcoll)

    To visualize data from init_pcoll once the pipeline is executed. And if you
    make change to the original pipeline by adding:

    squares = init_pcoll | 'Square' >> beam.Map(lambda x: x*x)

    When you re-run the pipeline from the line you just added, squares will
    use the init_pcoll data cached so you can have an interactive experience.

  Currently the implementation mainly watches for PCollection variables defined
  in user code. A watchable can be a dictionary of variable metadata such as
  locals(), a str name of a module, a module object or an instance of a class.
  The variable can come from any scope even local variables in a method of a
  class defined in a module.

    Below are all valid:

    watch(__main__)  # if import __main__ is already invoked
    watch('__main__')  # does not require invoking import __main__ beforehand
    watch(self)  # inside a class
    watch(SomeInstance())  # an instance of a class
    watch(locals())  # inside a function, watching local variables within
  """
  ie.current_env().watch(watchable)


def create_pipeline():
  """Creates a pipeline with interactive runner backed by direct runner.

  You can use run_pipeline() provided within this module to execute the iBeam
  pipeline with other runners.

  """
  return beam.Pipeline(interactive_runner.InteractiveRunner())


def run_pipeline(pipeline, runner, options=None):
  """Runs an iBeam pipeline with selected runner and options.

  When you are done with prototyping in an interactive notebook env, use this
  method to directly run the pipeline as a normal pipeline on your selected
  runner.

  Returns the url to job if it's Dataflow runner. None otherwise.
  Also returns the pipeline result object as the second value.
  """
  result = runner.run_pipeline(pipeline, options=options)
  if isinstance(runner, dataflow_runner.DataflowRunner):
    dataflow_options = options.view_as(GoogleCloudOptions)
    return ('https://console.cloud.google.com/dataflow/jobsDetail/locations/'
            '{}/jobs/{}?project={}'.format(dataflow_options.region,
                                           result.job_id(),
                                           dataflow_options.project)), result
  return None, result


def visualize(pcoll):
  """Visualizes a PCollection."""
  # TODO(BEAM-7926)
  pass
