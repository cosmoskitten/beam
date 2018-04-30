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
"""Support for installing custom code and required dependencies.

Workflows, with the exception of very simple ones, are organized in multiple
modules and packages. Typically, these modules and packages have
dependencies on other standard libraries. Dataflow relies on the Python
setuptools package to handle these scenarios. For further details please read:
https://pythonhosted.org/an_example_pypi_project/setuptools.html

When a runner tries to run a pipeline it will check for a --requirements_file
and a --setup_file option.

If --setup_file is present then it is assumed that the folder containing the
file specified by the option has the typical layout required by setuptools and
it will run 'python setup.py sdist' to produce a source distribution. The
resulting tarball (a .tar or .tar.gz file) will be staged at the GCS staging
location specified as job option. When a worker starts it will check for the
presence of this file and will run 'easy_install tarball' to install the
package in the worker.

If --requirements_file is present then the file specified by the option will be
staged in the GCS staging location.  When a worker starts it will check for the
presence of this file and will run 'pip install -r requirements.txt'. A
requirements file can be easily generated by running 'pip freeze -r
requirements.txt'. The reason a Dataflow runner does not run this automatically
is because quite often only a small fraction of the dependencies present in a
requirements.txt file are actually needed for remote execution and therefore a
one-time manual trimming is desirable.

TODO(silviuc): Staged files should have a job specific prefix.
To prevent several jobs in the same project stomping on each other due to a
shared staging location.

TODO(silviuc): Should we allow several setup packages?
TODO(silviuc): We should allow customizing the exact command for setup build.
"""

import functools
import logging
import os
import shutil
import tempfile

import pkg_resources

from apache_beam import version as beam_version
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.portability import stager

# All constants are for internal use only; no backwards-compatibility
# guarantees.

# In a released SDK, container tags are selected based on the SDK version.
# Unreleased versions use container versions based on values of
# BEAM_CONTAINER_VERSION and BEAM_FNAPI_CONTAINER_VERSION (see below).

# Update this version to the next version whenever there is a change that will
# require changes to legacy Dataflow worker execution environment.
BEAM_CONTAINER_VERSION = 'beam-master-20180413'
# Update this version to the next version whenever there is a change that
# requires changes to SDK harness container or SDK harness launcher.
BEAM_FNAPI_CONTAINER_VERSION = 'beam-master-20180413'

# Standard file names used for staging files.
WORKFLOW_TARBALL_FILE = 'workflow.tar.gz'
REQUIREMENTS_FILE = 'requirements.txt'
EXTRA_PACKAGES_FILE = 'extra_packages.txt'

# Package names for different distributions
GOOGLE_PACKAGE_NAME = 'google-cloud-dataflow'
BEAM_PACKAGE_NAME = 'apache-beam'

# SDK identifiers for different distributions
GOOGLE_SDK_NAME = 'Google Cloud Dataflow SDK for Python'
BEAM_SDK_NAME = 'Apache Beam SDK for Python'

DATAFLOW_CONTAINER_IMAGE_REPOSITORY = 'dataflow.gcr.io/v1beta3'


class DataflowFileHandle(stager.FileHandler):
  def file_copy(self, from_path, to_path):
    """Copies a local file to a GCS file or vice versa."""
    logging.info('file copy from %s to %s.', from_path, to_path)
    if from_path.startswith('gs://') or to_path.startswith('gs://'):
      from apache_beam.io.gcp import gcsio
      if from_path.startswith('gs://') and to_path.startswith('gs://'):
        # Both files are GCS files so copy.
        gcsio.GcsIO().copy(from_path, to_path)
      elif to_path.startswith('gs://'):
        # Only target is a GCS file, read local file and upload.
        with open(from_path, 'rb') as f:
          with gcsio.GcsIO().open(to_path, mode='wb') as g:
            pfun = functools.partial(f.read, gcsio.WRITE_CHUNK_SIZE)
            for chunk in iter(pfun, ''):
              g.write(chunk)
      else:
        # Source is a GCS file but target is local file.
        with gcsio.GcsIO().open(from_path, mode='rb') as g:
          with open(to_path, 'wb') as f:
            pfun = functools.partial(g.read, gcsio.DEFAULT_READ_BUFFER_SIZE)
            for chunk in iter(pfun, ''):
              f.write(chunk)
    else:
      # Branch used only for unit tests and integration tests.
      # In such environments GCS support is not available.
      if not os.path.isdir(os.path.dirname(to_path)):
        logging.info(
            'Created folder (since we have not done yet, and any errors '
            'will follow): %s ', os.path.dirname(to_path))
        os.mkdir(os.path.dirname(to_path))
      shutil.copyfile(from_path, to_path)


def stage_job_resources(
    options,
    file_copy=None,
    file_download=None,
    build_setup_args=None,
    temp_dir=None,
    populate_requirements_cache=None):
  """For internal use only; no backwards-compatibility guarantees.

  Creates (if needed) and stages job resources to options.staging_location.

  Args:
    options: Command line options. More specifically the function will expect
      staging_location, requirements_file, setup_file, and save_main_session
      options to be present.
    file_copy: Callable for copying files. The default version will copy from
      a local file to a GCS location using the gsutil tool available in the
      Google Cloud SDK package.
    build_setup_args: A list of command line arguments used to build a setup
      package. Used only if options.setup_file is not None. Used only for
      testing.
    temp_dir: Temporary folder where the resource building can happen. If None
      then a unique temp directory will be created. Used only for testing.
    populate_requirements_cache: Callable for populating the requirements cache.
      Used only for testing.

  Returns:
    A list of file names (no paths) for the resources staged. All the files
    are assumed to be staged in options.staging_location.

  Raises:
    RuntimeError: If files specified are not found or error encountered while
      trying to create the resources (e.g., build a setup package).
  """
  temp_dir = temp_dir or tempfile.mkdtemp()
  resources = []

  google_cloud_options = options.view_as(GoogleCloudOptions)
  setup_options = options.view_as(SetupOptions)
  # Make sure that all required options are specified. There are a few that have
  # defaults to support local running scenarios.
  if google_cloud_options.staging_location is None:
    raise RuntimeError('The --staging_location option must be specified.')
  if google_cloud_options.temp_location is None:
    raise RuntimeError('The --temp_location option must be specified.')

  file_handler = DataflowFileHandle()
  file_handler.file_copy = file_copy if file_copy else file_handler.file_copy
  file_handler.file_download = (
      file_download if file_download else file_handler.file_download)
  resource_stager = DataFlowStager(file_handler=file_handler)
  return resource_stager.stage_job_resources(
      options,
      build_setup_args=build_setup_args,
      temp_dir=temp_dir,
      populate_requirements_cache=populate_requirements_cache,
      staging_location=google_cloud_options.staging_location)


def get_runner_harness_container_image():
  """For internal use only; no backwards-compatibility guarantees.

   Returns:
     str: Runner harness container image that shall be used by default
       for current SDK version or None if the runner harness container image
       bundled with the service shall be used.
  """
  # Pin runner harness for released versions of the SDK.
  if 'dev' not in beam_version.__version__:
    return (DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/' + 'harness' + ':' +
            beam_version.__version__)
  # Don't pin runner harness for dev versions so that we can notice
  # potential incompatibility between runner and sdk harnesses.
  return None


def get_default_container_image_for_current_sdk(job_type):
  """For internal use only; no backwards-compatibility guarantees.

  Args:
    job_type (str): BEAM job type.

  Returns:
    str: Google Cloud Dataflow container image for remote execution.
  """
  # TODO(tvalentyn): Use enumerated type instead of strings for job types.
  if job_type == 'FNAPI_BATCH' or job_type == 'FNAPI_STREAMING':
    image_name = DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python-fnapi'
  else:
    image_name = DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python'
  image_tag = _get_required_container_version(job_type)
  return image_name + ':' + image_tag


def _get_required_container_version(job_type=None):
  """For internal use only; no backwards-compatibility guarantees.

  Args:
    job_type (str, optional): BEAM job type. Defaults to None.

  Returns:
    str: The tag of worker container images in GCR that corresponds to
      current version of the SDK.
  """
  if 'dev' in beam_version.__version__:
    if job_type == 'FNAPI_BATCH' or job_type == 'FNAPI_STREAMING':
      return BEAM_FNAPI_CONTAINER_VERSION
    else:
      return BEAM_CONTAINER_VERSION
  else:
    return beam_version.__version__


def get_sdk_name_and_version():
  """For internal use only; no backwards-compatibility guarantees.

  Returns name and version of SDK reported to Google Cloud Dataflow."""
  try:
    pkg_resources.get_distribution(GOOGLE_PACKAGE_NAME)
    return (GOOGLE_SDK_NAME, beam_version.__version__)
  except pkg_resources.DistributionNotFound:
    return (BEAM_SDK_NAME, beam_version.__version__)


class DataFlowStager(stager.Stager):

  def get_sdk_package_name(self):
    """For internal use only; no backwards-compatibility guarantees.

        Returns the PyPI package name to be staged to Google Cloud Dataflow."""
    sdk_name, _ = get_sdk_name_and_version()
    if sdk_name == GOOGLE_SDK_NAME:
      return GOOGLE_PACKAGE_NAME
    else:
      return BEAM_PACKAGE_NAME
