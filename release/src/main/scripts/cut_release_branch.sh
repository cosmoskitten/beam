#!/bin/bash
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

# This script will update apache beam master branch with next release version
# and cut release branch for current development version.

MASTER_BRANCH=master
RELEASE=2.6.0
DEV=2.6.0.dev
NEXT_VERSION_IN_BASE_BRANCH=2.7.0
RELEASE_BRANCH=release-${RELEASE}
GITHUB_REPO_URL=https://github.com/apache/beam.git
BEAM_ROOT_DIR=beam

# Enforce Release Manager input github credentials every time
git config --global --unset credential.helper

cd ~
git clone ${GITHUB_REPO_URL}
cd ${BEAM_ROOT_DIR}

# Create local release branch
git branch ${RELEASE_BRANCH}

git checkout ${MASTER_BRANCH}

# Update master branch
sed -i -e "s/'${RELEASE}'/'${NEXT_VERSION_IN_BASE_BRANCH}'/g" buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy
sed -i -e "s/${RELEASE}/${NEXT_VERSION_IN_BASE_BRANCH}/g" gradle.properties
sed -i -e "s/${RELEASE}/${NEXT_VERSION_IN_BASE_BRANCH}/g" sdks/python/apache_beam/version.py

echo "Update master branches as following: "
git diff

git add buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy
git add gradle.properties
git add sdks/python/apache_beam/version.py
git commit -m "Moving to ${NEXT_VERSION_IN_BASE_BRANCH}-SNAPSHOT on master branch."
git push origin ${MASTER_BRANCH}

# Checkout and update release branch
git checkout ${RELEASE_BRANCH}
sed -i -e "s/${DEV}/${RELEASE}/g" sdks/python/apache_beam/version.py
sed -i -e "s/beam-master-.*/beam-${RELEASE}/g" runners/google-cloud-dataflow-java/build.gradle

echo "Update release branch as following: "
git diff

git add sdks/python/apache_beam/version.py
git add runners/google-cloud-dataflow-java/build.gradle
git commit -m "Create release branch for version ${RELEASE}."
git push --set-upstream origin ${RELEASE_BRANCH}
