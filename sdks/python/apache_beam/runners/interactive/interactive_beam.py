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

import importlib

import apache_beam as beam
from apache_beam.runners.interactive import interactive_runner

"""Module of the current iBeam (interactive Beam) environment.

The purpose of the module is to reduce the learning curve of iBeam users, 
provide a single place for importing and add sugar syntax for all iBeam
components. It gives users capability to manipulate existing environment for
interactive beam, TODO(ningk) run interactive pipeline on selected runner as
normal pipeline, create pipeline with interactive runner and visualize
PCollections as bounded dataset.

Note: iBeam works the same as normal Beam with DirectRunner when not in an
interactively environment such as Jupyter lab or Jupyter Notebook. You can also
run pipeline created by iBeam as normal Beam pipeline by run_pipeline() with
desired runners.
"""

_ibeam_env = None


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
  current_env().watch(watchable)


def create_pipeline(runner=None, options=None, argv=None):
  """Creates a pipeline with interactive runner by default.

  You can use run_pipeline() provided within this module to execute the iBeam
  pipeline with other runners.

  Args:
    runner (~apache_beam.runners.runner.PipelineRunner): An object of
      type :class:`~apache_beam.runners.runner.PipelineRunner` that will be
      used to execute the pipeline. For registered runners, the runner name
      can be specified, otherwise a runner object must be supplied.
    options (~apache_beam.options.pipeline_options.PipelineOptions):
      A configured
      :class:`~apache_beam.options.pipeline_options.PipelineOptions` object
      containing arguments that should be used for running the Beam job.
    argv (List[str]): a list of arguments (such as :data:`sys.argv`)
      to be used for building a
      :class:`~apache_beam.options.pipeline_options.PipelineOptions` object.
      This will only be used if argument **options** is :data:`None`.

  Raises:
    ~exceptions.ValueError: if either the runner or options argument is not
      of the expected type.
  """
  if not runner:
    runner = interactive_runner.InteractiveRunner()
  return beam.Pipeline(runner, options, argv)


def visualize(pcoll):
  """Visualizes a PCollection."""
  # TODO(ningk)
  pass


def run_pipeline(pipeline, runner=None, options=None):
  """Runs an iBeam pipeline with selected runner and options.

  When you are done with prototyping in an interactive notebook env, use this
  method to directly run the pipeline as a normal pipeline on your selected
  runner.
  """
  # TODO(ningk)
  pass


def current_env(cache_manager=None):
  """Gets current iBeam environment."""
  global _ibeam_env
  if not _ibeam_env:
    _ibeam_env = InteractiveEnv(cache_manager)
  return _ibeam_env


def new_env(cache_manager=None):
  """Creates a new iBeam environment to replace current one."""
  global _ibeam_env
  _ibeam_env = None
  return current_env(cache_manager)


class InteractiveEnv(object):
  """An interactive environment with cache and pipeline variable metadata.

  iBeam will use the watched variable information to determine if a PCollection
  is ever assigned to a variable in user pipeline when executing the pipeline
  and apply magic to automatically cache and use cache for those PCollections
  if the pipeline is executed within an interactive environment. Users can also
  visualize and introspect those PCollections in user code since they have
  handlers to the variables.
  """

  def __init__(self, cache_manager=None):
    self._cache_manager = cache_manager
    # Holds class instances, module object, string of module names.
    self._watching_set = set()
    # Holds variables list of (Dict[str, object]).
    self._watching_dict_list = []
    # Always watch __main__ module.
    self.watch('__main__')

  def watch(self, watchable):
    """Watches a watchable.

    A watchable can be a dictionary of variable metadata such as locals(), a str
    name of a module, a module object or an instance of a class. The variable
    can come from any scope even local. Duplicated variable naming doesn't
    matter since they are different instances. Duplicated variables are also
    allowed when watching.
    """
    if isinstance(watchable, dict):
      self._watching_dict_list.append(watchable.items())
    else:
      self._watching_set.add(watchable)

  def watching(self):
    watching = list(self._watching_dict_list)
    for watchable in self._watching_set:
      if isinstance(watchable, str):
        module = importlib.import_module(watchable)
        watching.append(vars(module).items())
      else:
        watching.append(vars(watchable).items())
    return watching

  def set_cache_manager(self, cache_manager):
    self._cache_manager = cache_manager

  def cache_manager(self):
    return self._cache_manager
