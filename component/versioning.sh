#!/bin/bash

# Function to generate a version number
generate_version() {
  local version_file="version.txt"
  local major
  local minor

  # If version file doesn't exist, initialize it with 0.0
  if [ ! -e "$version_file" ]; then
    echo "0.0" > "$version_file"
  fi

  # Read the version from the file
  read major minor < "$version_file"

  # If the minor version is not 0, increment the major version and set minor to 0
  if [ "$minor" -ne 0 ]; then
    major=$((major + 1))
    minor=0
  else
    # Otherwise, increment the minor version
    minor=$((minor + 1))
  fi

  # Update the version file with the new version
  echo "$major $minor" > "$version_file"

  # Return the combined version string
  echo "$major.$minor"
}

# Update JSON config with the version
version=$(generate_version)
version_with_prefix="V${version}"
jq --arg ver "$version_with_prefix" '.script_paths.inference_image.imageTAG = $ver' build.json > temp.json && mv temp.json build.json
