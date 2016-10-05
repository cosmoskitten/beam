#!/bin/bash

mvn archetype:generate -pl sdks/java \
  -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
  -DarchetypeGroupId=org.apache.beam \
  -DarchetypeVersion=0.3.0-incubating-SNAPSHOT \
  -DgroupId=com.example \
  -DartifactId=test-beam-archetypes \
  -Dversion="0.3.0-incubating-SNAPSHOT" \
  -DinteractiveMode=false \
  -Dpackage=org.apache.beam.examples

cd test-beam-archetypes

mvn clean install
