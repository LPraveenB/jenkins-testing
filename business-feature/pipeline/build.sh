#!/bin/bash

ls component/business-feature/src/*.py
mydir=component/business-feature/src/
files="component/business-feature/src/*"
for f in $files;do
        echo "The variable's name is $f"
        #echo "The variable's content is ${!f}"
        gsutil cp $f gs://extracted-bucket-dollar-tree/Praveen/de-scripts/bfs/
done
