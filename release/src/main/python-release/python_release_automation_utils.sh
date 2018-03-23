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


set -e
set -v

#######################################
# Print Separators.
# Arguments:
#   Info to be printed.
# Outputs:
#   Writes info to stdout.
#######################################
function print_separator() {
    echo "############################################################################"
    echo $1
    echo "############################################################################"
}

#######################################
# Update gcloud version.
# Arguments:
#   None
#######################################
function update_gcloud() {
    curl https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-189.0.0-linux-x86_64.tar.gz \
    --output gcloud.tar.gz
    tar xf gcloud.tar.gz
    ./google-cloud-sdk/install.sh --quiet
    . ./google-cloud-sdk/path.bash.inc
    gcloud components update --quiet || echo 'gcloud components update failed'
    gcloud -v
}

#######################################
# Get Python SDK version from sdk/python/apache_beam/version.py.
# Arguments:
#   None
# Outputs:
#   Writes version to stdout.
#######################################
function get_version() {
    version=$(awk '/__version__/{print $3}' sdks/python/apache_beam/version.py)
    if [[ $version = *".dev"* ]]; then
        echo $version | cut -c 2- | rev | cut -d'.' -f2- | rev
    else
        echo $version
    fi
}

#######################################
# Publish data to Pubsub topic for streaming wordcount examples.
# Arguments:
#   None
#######################################
function run_pubsub_publish(){
    words=("hello world!", "I like cats!", "Python", "hello Python", "hello Python")
    for word in ${words[@]}; do
        gcloud pubsub topics publish $PUBSUB_TOPIC1 --message "$word"
    done
    sleep 10
}

#######################################
# Pull data from Pubsub.
# Arguments:
#   None
#######################################
function run_pubsub_pull() {
    gcloud pubsub subscriptions pull --project=$PROJECT_ID $PUBSUB_SUBSCRIPTION --limit=100 --auto-ack
}

#######################################
# Create Pubsub topics and subscription.
# Arguments:
#   None
#######################################
function create_pubsub() {
    gcloud pubsub topics create --project=$PROJECT_ID $PUBSUB_TOPIC1
    gcloud pubsub topics create --project=$PROJECT_ID $PUBSUB_TOPIC2
    gcloud pubsub subscriptions create --project=$PROJECT_ID $PUBSUB_SUBSCRIPTION --topic $PUBSUB_TOPIC2
}

#######################################
# Remove Pubsub topics and subscription.
# Arguments:
#   None
#######################################
function cleanup_pubsub() {
    gcloud pubsub topics delete --project=$PROJECT_ID $PUBSUB_TOPIC1
    gcloud pubsub topics delete --project=$PROJECT_ID $PUBSUB_TOPIC2
    gcloud pubsub subscriptions delete --project=$PROJECT_ID $PUBSUB_SUBSCRIPTION
}

#######################################
# Verify results of streaming_wordcount.
# Arguments:
#   $1 - runner type: DirectRunner, DataflowRunner
#   $2 - pid: the pid of running pipeline
#   $3 - running_job (DataflowRunner only): the job id of streaming pipeline running on DataflowRunner
#######################################
function verify_steaming_result() {
    retry=3
    should_see="Python: "
    while(( $retry > 0 )); do
        pull_result=$(run_pubsub_pull)
        if [[ $pull_result = *"$should_see"* ]]; then
            echo "SUCCEED: The streaming wordcount example running successfully on $1."
            break
        else
            if [[ $retry > 0 ]]; then
                retry=$(($retry-1))
                echo "retry left: $retry"
                sleep 15
            else
                echo "ERROR: The streaming wordcount example failed on $1."
                cleanup_pubsub
                kill -9 $2
                if [[ $1 = "DataflowRunner" ]]; then
                    gcloud dataflow jobs cancel $3
                fi
                complete "failed when running streaming wordcount example with $1."
                exit 1
            fi
        fi
    done
}

#######################################
# Verify results of user_score.
# Globals:
#   BUCKET_NAME
# Arguments:
#   $1: Runner - direct, dataflow
#######################################
function verify_user_score() {
    expected_output_file_name="$USERSCORE_OUTPUT_PREFIX-$1-runner.txt"
    actual_output_files=$(ls)
    if [[ $1 = *"dataflow"* ]]; then
        actual_output_files=$(gsutil ls gs://$BUCKET_NAME)
        expected_output_file_name="gs://$BUCKET_NAME/$expected_output_file_name"
    fi
    echo $actual_output_files
    if [[ $actual_output_files != *$expected_output_file_name* ]]
    then
        echo "ERROR: The userscore example failed on $1-runner".
        complete "failed when running userscore example with $1-runner."
        exit 1
    fi

    if [[ $1 = *"dataflow"* ]]; then
        gsutil rm $expected_output_file_name*
    fi
    echo "SUCCEED: user_score successfully run on $1-runner."
}

#######################################
# Verify results of hourly_team_score.
# Globals:
#   DATASET
# Arguments:
#   Runner - direct, dataflow
#######################################
function verify_hourly_team_score() {
    retry=3
    should_see='AntiqueBrassPlatypus'
    while(( $retry >= 0 )); do
        if [[ $retry > 0 ]]; then
            bq_pull_result=$(bq head -n 100 $DATASET.hourly_team_score_python_$1)
            if [[ $bq_pull_result = *"$should_see"* ]]; then
                echo "SUCCEED: hourly_team_score example successful run on $1-runner"
                break
            else
                retry=$(($retry-1))
                echo "Did not find team scores, retry left: $retry"
                sleep 15
            fi
        else
            echo "FAILED: HourlyTeamScore example failed running on $1-runner. \
                Did not found scores of team $should_see in $DATASET.leader_board"
            complete "FAILED"
            exit 1
        fi
    done
}



# Python RC configurations
#VERSION=$(get_version)
VERSION=2.4.0
RC_STAGING_URL="https://dist.apache.org/repos/dist/dev/beam/$VERSION/"
BEAM_PYTHON_SDK_ZIP="apache-beam-$VERSION-python.zip"
BEAM_PYTHON_SDK_WHL="apache_beam-$VERSION.rc3-cp27-cp27mu-manylinux1_x86_64.whl"

# Cloud Configurations
PROJECT_ID='apache-beam-testing'
BUCKET_NAME='temp-storage-for-release-validation-tests/nightly-snapshot-validation'
TEMP_DIR='/tmp'
DATASET='beam_postrelease_mobile_gaming'
NUM_WORKERS=1

# Quickstart configurations
TAR_SHA512_FILE_NAME="apache-beam-$VERSION-python.zip.sha512"
TAR_ASC_FILE_NAME="apache-beam-$VERSION-python.zip.asc"
WHL_SHA512_FILE_NAME="apache-beam-$VERSION.rc3-cp27-cp27mu-manylinux1_x86_64.whl.sha512"
WHL_ASC_FILE_NAME="apache-beam-$VERSION.rc3-cp27-cp27mu-manylinux1_x86_64.whl.asc"

WORDCOUNT_OUTPUT='wordcount_direct.txt'
PUBSUB_TOPIC1='wordstream-python-topic-1'
PUBSUB_TOPIC2='wordstream-python-topic-2'
PUBSUB_SUBSCRIPTION='wordstream-python-sub2'

# Mobile Gaming Configurations
DATASET='beam_postrelease_mobile_gaming'
USERSCORE_OUTPUT_PREFIX='python-userscore_result'
