
FROM python:${PYTHON_VERSION}

# Install jq
RUN apt-get update &&     apt-get install -y jq &&     apt-get clean

# Install pip.
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py &&     python get-pip.py &&     pip3 install setuptools &&     rm get-pip.py
RUN python3 -m pip install --upgrade pip

COPY build.json /app/

# Install packages using versions from the config file
WORKDIR /app

RUN python3 -m pip install -U \
    pyarrow==11.0.0 fastparquet==2023.2.0 gcsfs==2023.3.0 xgboost==1.7.0 dask-ml==2023.3.24 scikit-learn==1.2.2 pandas==1.5.3 bokeh==2.4.0 seaborn==0.13.0 nbformat==5.9.2 nbconvert==7.9.2 papermill==2.4.0

# Remove the JSON config file
RUN rm -rf build.json

# Remove additional files or directories if needed
 RUN rm -rf inference

 RUN mkdir -p inference
COPY * inference/
WORKDIR inference
RUN ls

ENTRYPOINT ["bash", "build_inference.sh"]
