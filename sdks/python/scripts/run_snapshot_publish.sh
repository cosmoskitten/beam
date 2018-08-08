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


VERSION=$(awk '/__version__/{print $3}' $WORKSPACE/src/sdks/python/apache_beam/version.py)
VERSION=$(echo $VERSION | cut -c 2- | rev | cut -c 2- | rev)
time=$(date +"%Y-%m-%dT%H:%M:%S")
SNAPSHOT="apache-beam-$VERSION-$time.zip"
BUCKET=gs://beam-python-nightly-snapshots

cd $WORKSPACE/src/sdks/python/build

# rename the file to be apache-beam-{VERSION}-{datetime}.zip
for file in "apache-beam-$VERSION*.zip"; do
  mv $file $SNAPSHOT
done

#generate sha512 hash
sha512sum $SNAPSHOT > "$SNAPSHOT.sha512"

ls
#generate gpg key
wget https://dist.apache.org/repos/dist/dev/beam/KEYS
gpg --import KEYS
gpg --armor --detach-sig $SNAPSHOT

ls


gsutil cp $SNAPSHOT $BUCKET/$VERSION/
gsutil cp *.sha512  $BUCKET/$VERSION/
gsutil cp *.asc     $BUCKET/$VERSION/
