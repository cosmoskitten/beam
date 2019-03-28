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

"""Dataflow credentials and authentication."""

from __future__ import absolute_import

import datetime
import json
import logging
import os

from future.moves.urllib.request import Request
from future.moves.urllib.request import urlopen
from oauth2client.client import GoogleCredentials
from oauth2client.client import OAuth2Credentials

from apache_beam.utils import retry

# When we are running in GCE, we can authenticate with VM credentials.
is_running_in_gce = False

# When we are running in GCE, this value is set based on worker startup
# information.
executing_project = None


def set_running_in_gce(worker_executing_project):
  """For internal use only; no backwards-compatibility guarantees.

  Informs the authentication library that we are running in GCE.

  When we are running in GCE, we have the option of using the VM metadata
  credentials for authentication to Google services.

  Args:
    worker_executing_project: The project running the workflow. This information
      comes from worker startup information.
  """
  global is_running_in_gce
  global executing_project
  is_running_in_gce = True
  executing_project = worker_executing_project


class AuthenticationException(retry.PermanentException):
  pass


class _GCEMetadataCredentials(OAuth2Credentials):
  """For internal use only; no backwards-compatibility guarantees.

  Credential object initialized using access token from GCE VM metadata."""

  def __init__(self, user_agent=None):
    """Create an instance of GCEMetadataCredentials.

    These credentials are generated by contacting the metadata server on a GCE
    VM instance.

    Args:
      user_agent: string, The HTTP User-Agent to provide for this application.
    """
    super(_GCEMetadataCredentials, self).__init__(
        None,  # access_token
        None,  # client_id
        None,  # client_secret
        None,  # refresh_token
        datetime.datetime(2010, 1, 1),  # token_expiry, set to time in past.
        None,  # token_uri
        user_agent)

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _refresh(self, http_request):
    refresh_time = datetime.datetime.utcnow()
    metadata_root = os.environ.get(
        'GCE_METADATA_ROOT', 'metadata.google.internal')
    token_url = ('http://{}/computeMetadata/v1/instance/service-accounts/'
                 'default/token').format(metadata_root)
    req = Request(token_url, headers={'Metadata-Flavor': 'Google'})
    token_data = json.loads(urlopen(req).read().decode('utf-8'))
    self.access_token = token_data['access_token']
    self.token_expiry = (refresh_time +
                         datetime.timedelta(seconds=token_data['expires_in']))


def get_service_credentials():
  """For internal use only; no backwards-compatibility guarantees.

  Get credentials to access Google services."""
  user_agent = 'beam-python-sdk/1.0'
  if is_running_in_gce:
    # We are currently running as a GCE taskrunner worker.
    #
    # TODO(ccy): It's not entirely clear if these credentials are thread-safe.
    # If so, we can cache these credentials to save the overhead of creating
    # them again.
    return _GCEMetadataCredentials(user_agent=user_agent)
  else:
    client_scopes = [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/devstorage.full_control',
        'https://www.googleapis.com/auth/userinfo.email',
        'https://www.googleapis.com/auth/datastore'
    ]

    try:
      credentials = GoogleCredentials.get_application_default()
      credentials = credentials.create_scoped(client_scopes)
      logging.debug('Connecting using Google Application Default '
                    'Credentials.')
      return credentials
    except Exception as e:
      logging.warning(
          'Unable to find default credentials to use: %s\n'
          'Connecting anonymously.', e)
      return None
