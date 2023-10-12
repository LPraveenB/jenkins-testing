#!/bin/bash

CONFIG_FILE="build.json"
PYTHON_VERSION=$(jq -r '.script_paths.inference_image.pythonVersion' $CONFIG_FILE)
PACKAGES=$(cat build.json | jq -r '.package_versions | to_entries | .[] | "\(.key)==\(.value)"')
PACKAGES_INSTALL=$(echo $PACKAGES | tr ' ' '\n' | xargs echo)

cat <<EOF > Dockerfile
ARG PYTHON_VERSION="$PYTHON_VERSION"
FROM python:\${PYTHON_VERSION}

# Install jq
RUN apt-get update && \
    apt-get install -y jq && \
    apt-get clean

# Install pip.
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python get-pip.py && \
    pip3 install setuptools && \
    rm get-pip.py
RUN python3 -m pip install --upgrade pip

COPY build.json /app/

# Install packages using versions from the config file
WORKDIR /app

RUN python3 -m pip install -U \\
    $PACKAGES_INSTALL

# Remove the JSON config file
RUN rm -rf build.json

# Remove additional files or directories if needed
 RUN rm -rf inference

 RUN mkdir -p inference
COPY * inference/
WORKDIR inference
RUN ls

ENTRYPOINT ["bash", "build_inference.sh"]
EOF