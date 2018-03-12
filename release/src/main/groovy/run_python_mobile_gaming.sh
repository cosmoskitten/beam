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

#  This file will verify Apache/Beam release candidate python by following steps:
#
#  1. Create a new virtualenv and install the SDK
#  2. Run UserScore examples with DirectRunner
#  3. Run UserScore examples with DataflowRunner
#  4. Run HourlyTeamScore on DirectRunner
#  5. Run HourlyTeamScore on DataflowRunner
#  6. Run LeaderBoard on DirectRunner
#

set -e
set -v

source release/src/main/groovy/python_release_automation_utils.sh

complete() {
    print_separator "Validation $1"
    rm -rf $TMPDIR
}

verify_houly_team_score() {
    # $1 runner type
    retry=3
    should_see='AntiqueBrassPlatypus'
    while(( $retry >= 0 ))
    do
        if [[ $retry > 0 ]]
        then
            bq_pull_result=$(bq head -n 100 $DATASET.hourly_team_score_python_$1)
            if [[ $bq_pull_result = *"$should_see"* ]]
            then
                echo "SUCCEED: HourlyTeamScore example successful run on $1-runner"
                break
            else
                retry=$(($retry-1))
                echo "Did not find team scores, retry left: $retry"
                sleep 15
            fi
        else
            echo "FAILED: HourlyTeamScore example failed running on $1-runner. Did not found scores of team $should_see in $DATASET.leader_board"
            complete "FAILED"
            exit 1
        fi
    done
}


print_separator "Start Mobile Gaming Examples"
echo "SDK version: $VERSION"

TMPDIR=$(mktemp -d)
echo $TMPDIR
pushd $TMPDIR

#
# 1. Download files from RC staging location, install python sdk
#

wget $CANDIDATE_URL$BEAM_PYTHON_SDK
print_separator "Creating new virtualenv and installing the SDK"
virtualenv temp_virtualenv
. temp_virtualenv/bin/activate && pip install $BEAM_PYTHON_SDK[gcp]
gcloud_version=$(gcloud --version | head -1 | awk '{print $4}')
if [[ "$gcloud_version" < "189" ]]
then
  update_gcloud
fi


#
# 2. Run UserScore with DirectRunner
#

print_separator "Running userscore example with DirectRunner"
output_file_name="$USERSCORE_OUTPUT_PREFIX-direct-runner.txt"
python -m apache_beam.examples.complete.game.user_score \
--output=$output_file_name \
--project=$PROJECT_ID \
--dataset=$DATASET \
--input=gs://$BUCKET_NAME/5000_gaming_data.csv
if ls $output_file_name* 1> /dev/null 2>&1;
then
	echo "Found output file(s):"
	ls $output_file_name*
else
	echo "ERROR: output file not found."
	complete "failed when running userscore example with DirectRunner."
	exit 1
fi
echo "SUCCEED: UserScore successfully run on DirectRunner."


#
# 3. Run UserScore with DataflowRunner
#

print_separator "Running userscore example with DataflowRunner"
output_file_name="$USERSCORE_OUTPUT_PREFIX-dataflow-runner.txt"
python -m apache_beam.examples.complete.game.user_score \
--project=$PROJECT_ID \
--runner=DataflowRunner \
--temp_location=gs://$BUCKET_NAME/temp/ \
--sdk_location=$BEAM_PYTHON_SDK \
--input=gs://$BUCKET_NAME/5000_gaming_data.csv \
--output=gs://$BUCKET_NAME/$output_file_name
# verify results.
userscore_output_in_gcs="gs://$BUCKET_NAME/$output_file_name"
gcs_pull_result=$(gsutil ls gs://$BUCKET_NAME)
if [[ $gcs_pull_result != *$userscore_output_in_gcs* ]]
then
    echo "ERROR: The userscore example failed on DataflowRunner".
    complete "failed when running userscore example with DataflowRunner."
    exit 1
fi
gsutil rm gs://$BUCKET_NAME/$output_file_name*
echo "SUCCEED: UserScore successfully run on DataflowRunner."


#
# 4. Run HourlyTeamScore with DirectRunner
#

print_separator "Running HourlyTeamScore example with DirectRunner"
python -m apache_beam.examples.complete.game.hourly_team_score \
--project=$PROJECT_ID \
--dataset=$DATASET \
--input=gs://$BUCKET_NAME/5000_gaming_data.csv \
--table="hourly_team_score_python_direct"

verify_houly_team_score "direct"


#
# 5. Run HourlyTeamScore with DataflowRunner
#

print_separator "Running HourlyTeamScore example with DataflowRunner"
python -m apache_beam.examples.complete.game.hourly_team_score \
--project=$PROJECT_ID \
--dataset=$DATASET \
--runner=DataflowRunner \
--temp_location=gs://$BUCKET_NAME/temp/ \
--sdk_location $BEAM_PYTHON_SDK \
--input=gs://$BUCKET_NAME/5000_gaming_data.csv \
--table="hourly_team_score_python_dataflow"

verify_houly_team_score "dataflow"


#
# 6. Run LeaderBoard with DirectRunner
#

print_separator "Running LeaderBoard example with DirectRunner"

pwd
popd
pwd
ls

complete "SUCCEED: Mobile Gaming Verification Complete"
