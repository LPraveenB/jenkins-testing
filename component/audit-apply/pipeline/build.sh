#!/bin/bash

build_json="${WORKSPACE}/component/build.json"
scripts_dest=$(jq -r .scripts_path.audit_apply "$build_json")
src_file="${WORKSPACE}/component/audit-apply/*"
gsutil -m cp -r "$src_files" "$scripts_dest"

