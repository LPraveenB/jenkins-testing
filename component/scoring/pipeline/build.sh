#!/bin/bash

build_json="${WORKSPACE}/component/build.json"
scripts_dest=$(jq -r .script_paths.scoring "$build_json")

ls ${WORKSPACE}/component/scoring/src/*.py
src_dir="${WORKSPACE}/component/scoring/"
gsutil -m cp -r "$src_dir" "$scripts_dest"