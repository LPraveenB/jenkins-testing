#!/bin/bash

build_json="${WORKSPACE}/component/build.json"
scripts_dest=$(jq -r .scripts_path.audit_apply "$build_json")
src_dir="${WORKSPACE}/component/audit-apply/"
gsutil -m cp -r "$src_dir" "$scripts_dest"

