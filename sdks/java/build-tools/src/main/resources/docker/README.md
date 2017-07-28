<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

Docker Images
========================

This directory contains a set of scripts to build docker images of Apache Beam
for different purposes:

- **file**: Create a docker container from a .zip file with the Beam in a given
  environment. It is useful to test/validate the release votes or an specific
  version in a different version of Java than the host. It reproduces a minimal
  docker image so it also isolate issues due to ports shared in the machine,
  etc.

- **git**: Same as file but the Beam source code comes from the git repository,
  you can choose a given branch/tag/pull-request. Useful to test in a specific
  environment.

- **release**: It builds an end-user distribution of the latest version of Beam
  and its dependencies. Useful for end-users who want to have a ready to use
  container with Beam (Python only for the moment).

# File based image

If you want to build a container with a ready JDK 8 environment to test Beam:

    cd file/openjdk8
    docker build -t beam:openjdk8 .

If you want to build a container with JDK 7:

    cd file/openjdk7
    docker build -t beam:openjdk7 .

When you run the image it downloads the specific version of Beam given the
environment variables, by default it downloads the source code of the latest
master, if you want to download the latest master and execute the tests:

    docker run -it beam:openjdk8 mvn clean verify -Prelease

If you want to have an interactive session you can run a bash prompt for example
to test manually a specific module:

    docker run -it beam:openjdk8 /bin/bash
    mvn --projects sdks/java/io/hbase clean verify -Prelease

## Configuring the runtime via the environment variables

You can run different versions of Beam by passing the specific environment
variables:

`URL`: The URL with the file containing the specific source code.  
`SRC_FILE`: The downloaded file name.  
`SRC_DIR`: The name of the directory inside of the zip file.

For example to run a docker container with the exact code of the release 2.0.0:

    docker run \
      -e URL="https://www.apache.org/dyn/closer.cgi?filename=beam/2.0.0/apache-beam-2.0.0-source-release.zip&action=download" \
      -e SRC_FILE="apache-beam-2.0.0-source-release.zip" \
      -e SRC_DIR="apache-beam-2.0.0" \
      -it beam:openjdk8 /bin/bash

If you want to run a container with a specific version (e,g, 2.1.0-RC1):

    docker run \
      -e URL="https://github.com/apache/beam/archive/v2.1.0-RC1.zip" \
      -e SRC_FILE="v2.1.0-RC1.zip" \
      -e SRC_DIR="beam-2.1.0-RC1" \
      -it beam:openjdk8 /bin/bash

*Notice that SRC_FILE is different from SRC_DIR because github replaces the 'v'
character in the internal name of the zip*.

To run a container with the source code during a vote:

    docker run \
      -e URL="https://dist.apache.org/repos/dist/dev/beam/2.1.0/apache-beam-2.1.0-source-release.zip" \
      -e SRC_FILE="apache-beam-2.1.0-source-release.zip" \
      -e SRC_DIR="apache-beam-2.1.0" \
      -it beam:openjdk8 /bin/bash

## Testing in an specific environment with your own source

You can also overwrite the volume containing the source code of Beam from a
directory in your host machine. This is useful to test your code in different
versions of Java.

If you have the code in the ~/workspace/beam directory and you want to quickly
test it with Beam on Java 7 you can do:

    docker run -v ~/workspace/beam:/home/user/beam -it beam:openjdk7 /bin/bash

## Performance improvements

The docker image does **not** contain the java dependencies and this could make
the execution of the commands in the image take more time than expected. One way
to speed this is by mounting a directory with the dependencies as a docker
volume. The following command shows how to do this locally from the host machine
mapping the local maven's `~/.m2` directory in the container.

    docker run -v ~/.m2:/home/user/.m2 -it beam:openjdk8 /bin/bash

You can also use an empty directory instead of `~/.m2` if you want to create a
different environment from the local one to share it between runs.

**Maven Improvements:**

You can also speed the execution by adding environment variables to maven, for
example the MAVEN_OPTS environment variable may speed up compilation:
 
    docker run \
      -e MAVEN_OPTS='-client -XX:+TieredCompilation -XX:TieredStopAtLevel=1 -Xverify:none' \
      -it beam:openjdk8 /bin/bash

Maven does not execute on multiple cores by default, to do this you have to
enable the threading execution:
    
    mvn --threads 1C ...

## Python

The python version of the file container differs from the other versions because
it only contains the python source code, if you want to build the python image
based on the latest release you can run:

    cd file/python2
    docker build -t beam:python2 .

You can then run the python tests or install it

    python setup.py install --user
    python -m apache_beam.examples.wordcount --input <PATH_TO_INPUT_FILE> --output counts

*Note that this image does not create an additional virtualenv container.*

# Git based image

It creates a docker container with a cloned version of the git repository, its
branches and all the pull-requests. When it is run, it updates and checks out
the specified branch/tag/pull-request.

You can choose the branch to execute via the environment variable:

`BRANCH`: The name of the specific branch, tag or pr to check out.

By default it checks out master from the github repo, but if you want to check
for example a specific pull request ID you can build the image like this (you
must change ID for the number):

    cd git/openjdk8
    docker build -t beam:git .
    docker run -e BRANCH=pr/ID -it beam:git /bin/bash

# Release image

The release image is a container with the needed requirements to run Beam out of
the box in a container, or to submit a pipeline to execute on Google Cloud
Dataflow to build it you just do:

    cd release/python2
    docker build -t beam:latest-python2 .
