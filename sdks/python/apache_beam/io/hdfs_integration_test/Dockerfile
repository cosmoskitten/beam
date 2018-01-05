###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
###############################################################################

# This image contains a Python SDK build and dependencies, including hdfs3
# dependencies. It uses a conda virtual environment with conda and pip packages.
#
# Use hdfs_integration_test.sh to build.

FROM conda/miniconda2
WORKDIR /app

# Create virtual environment and install libhdfs3.
RUN conda create -n beamenv python && \
    echo "source activate beamenv" >> ~/.bashrc && \
    /bin/bash -cl 'conda install -c conda-forge hdfs3'

# Install Beam and dependencies.
ADD sdks/python /app/sdks/python
ADD model /app/model
RUN /bin/bash -cl 'cd sdks/python && \
    python setup.py sdist && \
    pip install $(ls dist/apache-beam-*.tar.gz)[gcp]'

# Add environment variable and config file for hdfs3 auto-configuration.
ENV HADOOP_CONF_DIR /app/etc/hadoop
COPY sdks/python/apache_beam/io/hdfs_integration_test/core-site.xml \
    $HADOOP_CONF_DIR/

# Run wordcount, and write results to HDFS.
CMD bash -cl 'python -m apache_beam.examples.wordcount \
        --output hdfs://tmp/py-wordcount-direct'
