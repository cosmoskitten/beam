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

# Minimal HDFS service for integration testing.
#
# Use hdfs_integration_test.sh to build and debug.

FROM openjdk:8
WORKDIR /app

RUN apt-get update && \
    apt-get install -y \
        dropbear-run \
    && rm -rf /var/lib/apt/lists/*

# Install Hadoop
ENV HVERSION "hadoop-2.9.0"
ENV HFILE "$HVERSION.tar.gz"
ENV HFILE_SHA512 "677D6FC87A75F9E368D65074D42C5F0A947AEFE1D2115C2F36369802C76463E9671167FF7876A27EE031A732177E21D48A63D913940B7F4CE853C030A8B7CA92"
RUN curl http://mirrors.sonic.net/apache/hadoop/common/$HVERSION/$HFILE \
    --output $HFILE && \
    echo -n Verifying hash... && \
    sha512sum $HFILE | \
    grep -iq $HFILE_SHA512 && \
    echo OK && \
    tar xf $HFILE && \
    rm $HFILE

# Setup passwordless ssh (needed for start-dfs.sh)
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    echo StrictHostKeyChecking=no >> ~/.ssh/config

RUN echo "export JAVA_HOME=${JAVA_HOME}" >> ~/.bashrc

# Install Miniconda
ENV CFILE "Miniconda2-4.3.30-Linux-x86_64.sh"
ENV CFILE_MD5 "bd1655b4b313f7b2a1f2e15b7b925d03"
ENV CONDA_PATH "/app/miniconda"
RUN curl https://repo.continuum.io/miniconda/$CFILE \
    --output $CFILE && \
    echo -n Verifying hash... && \
    md5sum $CFILE | \
    grep -iq $CFILE_MD5 && \
    echo OK && \
    bash $CFILE -b -p $CONDA_PATH && \
    rm $CFILE && \
    echo "export PATH=\"$CONDA_PATH/bin:$PATH\"" >> ~/.bashrc

# Setup virtual environment.
RUN export PATH="$CONDA_PATH/bin:$PATH" && \
    conda create -n beamenv python && \
    echo "source activate beamenv" >> ~/.bashrc && \
    /bin/bash -cl 'conda install -c conda-forge hdfs3'

# HTTP and HDFS ports, for debugging.
EXPOSE 50070 9000

ADD . /app
ENV HADOOP_CONF_DIR /app/$HVERSION/etc/hadoop
COPY sdks/python/apache_beam/io/hdfs_container/*.xml $HADOOP_CONF_DIR/
RUN $HVERSION/bin/hdfs namenode -format

# Install Beam and dependencies.
RUN /bin/bash -cl 'cd sdks/python && \
    python setup.py sdist && \
    pip install $(ls dist/apache-beam-*.tar.gz)[gcp]'

# TODO: remove cat /etc/resolv.conf
CMD bash -cl 'cat /etc/resolv.conf && \
    service dropbear start && \
    $HVERSION/sbin/start-dfs.sh && \
    python -m apache_beam.examples.wordcount \
        --output hdfs://tmp/py-wordcount-direct'
