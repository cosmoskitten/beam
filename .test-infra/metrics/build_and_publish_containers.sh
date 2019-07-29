#!/bin/bash
echo
echo ===========Start==========
CONTAINER_VERSION_NAME=$1
DO_PUSH=$2

echo
echo ===========Building containers==========
docker build -t gcr.io/${PROJECT_ID}/beamgrafana:$CONTAINER_VERSION_NAME ./grafana
docker build -t gcr.io/${PROJECT_ID}/beammetricssyncjenkins:$CONTAINER_VERSION_NAME ./sync/jenkins
docker build -t gcr.io/${PROJECT_ID}/beammetricssyncjira:$CONTAINER_VERSION_NAME ./sync/jira
docker build -t gcr.io/${PROJECT_ID}/beammetricssyncgithub:$CONTAINER_VERSION_NAME ./sync/github

if [ "$DO_PUSH" = true ]; then
  echo
  echo ===========Publishing containers==========
  docker push gcr.io/${PROJECT_ID}/beamgrafana:$CONTAINER_VERSION_NAME
  docker push gcr.io/${PROJECT_ID}/beammetricssyncjenkins:$CONTAINER_VERSION_NAME
  docker push gcr.io/${PROJECT_ID}/beammetricssyncjira:$CONTAINER_VERSION_NAME
  docker push gcr.io/${PROJECT_ID}/beammetricssyncgithub:$CONTAINER_VERSION_NAME
fi

echo
echo ===========Updating deployment script==========
set -o xtrace
sed -i "s/\( *image: gcr.io\/${PROJECT_ID}\/beam.*:\).*$/\1$CONTAINER_VERSION_NAME/g" ./beamgrafana-deploy.yaml
set +o xtrace

