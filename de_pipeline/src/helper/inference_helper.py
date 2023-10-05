from ghost_calc_pipelines.components.de_pipeline.src import constants as constant
from ghost_calc_pipelines.components.de_pipeline.src.helper.helper import Helper
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
import json
from urllib.parse import urlparse
import logging
from datetime import datetime
from google.cloud import storage
import re
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator
)
from airflow.models import Variable


@task.virtualenv(
    task_id="inference",
    requirements=["kfp==2.0.1", "protobuf==3.20.3", "kfp-pipeline-spec==0.2.2", "google-cloud-aiplatform==1.24.1",
                  "google-cloud-storage==2.10.0"],
    system_site_packages=False,
    op_kwargs={"run_date":Variable.get(key="run_date")}
)
def inference(location_groups,**context):
    """
    Example function that will be performed in a virtual environent.

    Importing at the module level ensures that it will not attempt to import the
    library before it is installed.
    """
    from time import sleep
    import subprocess
    import json

    from google.cloud import aiplatform
    from google.cloud import storage
    # from google_cloud_pipeline_components.v1.custom_job import create_custom_training_job_from_component
    import kfp
    import kfp.v2.dsl as dsl
    from kfp.v2.dsl import component, Output, HTML

    import json
    import os
    from datetime import datetime
    from google.cloud import aiplatform
    from random import randint
    # from google_cloud_pipeline_components.v1.custom_job import create_custom_training_job_from_component
    import kfp
    import kfp.v2.dsl as dsl
    from kfp.v2.dsl import component, Output, HTML

    """
    credential_path = "/opt/airflow/dags/ghost_calc_pipelines/components/de_pipeline/src/dollar-tree-project-369709-675d6b871fe6.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    os.environ['GOOGLE_CLOUD_PROJECT'] = "dollar-tree-project-369709"
    """
    # Instantiate a Google Cloud Storage client and specify required bucket and file
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('vertex-scripts')
    blob = bucket.blob('ml-scripts/inference.json')

    # Download the contents of the blob as a string and then parse it using json.loads() method
    data = json.loads(blob.download_as_string(client=None))
    print(" ********************** inference data ")
    print(data)
    BUCKET_URI = data.get("bucket_uri")
    aiplatform.init(project=data.get("project_id"), staging_bucket=data.get("bucket_uri"), location=data.get("region"))

    @component(
        output_component_file="demo_component.yaml",
        base_image="python:3.7",
        packages_to_install=["google-cloud-aiplatform==1.24.1"],
    )
    def fd_inference(location_groups: list = [],config_data:dict={},run_date:str=None) -> str:
        def get_infra_specs(location_groups: list,config_data:dict,run_date:str):
            CONTAINER_TRAIN_IMAGE = config_data.get("CONTAINER_TRAIN_IMAGE")
            worker_pool_specs = [{
                'machine_spec': {
                    'machine_type': config_data.get("machine_type"),
                },
                'replica_count': 1,
                'container_spec': {
                    'image_uri': config_data.get("CONTAINER_TRAIN_IMAGE"),
                    "args": [
                        '--source_path_model_s1',
                        config_data.get("source_path_model_s1"),
                        '--source_path_model_s2',
                        config_data.get("source_path_model_s2"),
                        '--source_path_model_s3',
                        config_data.get("source_path_model_s3"),
                        '--source_path_model_s4',
                        config_data.get("source_path_model_s4"),
                        '--decision_threshold_step_1', config_data.get("decision_threshold_step_1"),
                        '--decision_threshold_step_2', config_data.get("decision_threshold_step_1"),
                        '--business_feature_store_base_path',
                        config_data.get("business_feature_store_base_path"),
                        '--output_path', config_data.get("output_path"),
                        '--location_group_list', " ".join(location_groups),
                        '--load_date', run_date,
                        '--dask_address', config_data.get("dask_address"),
                        '--num_workers_local_cluster', config_data.get("num_workers_local_cluster"),
                        '--num_threads_per_worker', config_data.get("num_threads_per_worker"),
                        '--memory_limit_local_worker', config_data.get("num_threads_per_worker"),
                        '--dask_connection_timeout', config_data.get("dask_connection_timeout"),
                        '--local_dask_flag', config_data.get("local_dask_flag")
                    ]
                },
            }]
            return worker_pool_specs

        def train(job_spec,config_data=config_data):
            from google.cloud import aiplatform
            from datetime import datetime
            from random import randint

            my_job = aiplatform.CustomJob(
                display_name="inference" + "-" + datetime.now().strftime("%f")+"-"+str(randint(1,100)),
                worker_pool_specs=job_spec,
                staging_bucket=config_data.get("staging_bucket"),
                project=config_data.get("project_id"),
                location=config_data.get("region")
            )

            my_job.run(service_account="prod-vm-win-darshan@dollar-tree-project-369709.iam.gserviceaccount.com")

        specs = get_infra_specs(location_groups=location_groups,config_data=config_data,run_date=run_date)
        train(specs,config_data=config_data)

        return "success"

    PIPELINE_ROOT = "gs://{}/darshan/raw_data/machine_settings".format(BUCKET_URI)

    CPU_LIMIT = "16"  # vCPUs
    MEMORY_LIMIT = "100G"

    @dsl.pipeline(
        name="component-fd-custom-inference",
        description="A simple pipeline that requests component-level machine resource",
        pipeline_root=PIPELINE_ROOT,
    )
    def pipeline(location_groups: list = [],config_data:dict={},run_date:str=None):
        training_job_task = (
            fd_inference(location_groups=location_groups,config_data=config_data,run_date=run_date)
            .set_display_name("fd-inference" + "-" + datetime.now().strftime("%f"))
            .set_cpu_limit(CPU_LIMIT)
            .set_memory_limit(MEMORY_LIMIT)
        )

    def main(location_groups: list, config_data:dict,run_date:str):
        print(" Running Kubeflow pipeline ** " + " ".join(location_groups))
        kfp.v2.compiler.Compiler().compile(
            pipeline_func=pipeline,
            pipeline_parameters={"location_groups": location_groups, "config_data": config_data,"run_date":run_date},
            package_path="component_level_settings.json",
        )

        aipipeline = aiplatform.PipelineJob(
            display_name="component-level-settings",
            template_path="component_level_settings.json",
            pipeline_root=PIPELINE_ROOT,
            enable_caching=False,
        )

        aipipeline.run()
        return location_groups

    main(location_groups=location_groups, config_data=data,run_date=context.get("run_date"))
    return location_groups
