#!/usr/bin/env sh

echo "Provided arguments:" "$@"

args=("$@")

declare -a mappings

# Load project-mappings file into mappings.
let i=0
while IFS=$'\n' read -r line_data; do
    mappings[i]="${line_data}"
    ((++i))
done < project-mappings

for i in "${!args[@]}"
do
  for mapping in "${mappings[@]}"
  do
     map=($mapping)
     args[$i]=${args[$i]/${map[0]}/${map[1]}}
  done
done

echo "Passing to gradle script:"  "${args[@]}"

./gradlew_orig "${args[@]}"