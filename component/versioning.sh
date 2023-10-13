#!/bin/bash

# Function to generate a version number
generate_version() {
  local version=$(date +'%Y%m%d%H')
  echo "$version"
}

# Update JSON config with the version
version=$(generate_version)
jq --arg ver "$version" '.script_paths.inference_image.imageTAG = $ver' build.json > temp.json && mv temp.json build.json
