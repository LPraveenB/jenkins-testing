#!/bin/bash

ls business-feature/src/*.py
mydir=business-feature/src/
files="business-feature/src/*"
for f in $files;do
        echo "The variable's name is $f"
        #echo "The variable's content is ${!f}"
        gsutil cp $f gs://extracted-bucket-dollar-tree/Praveen/de-scripts/bfs/
done
