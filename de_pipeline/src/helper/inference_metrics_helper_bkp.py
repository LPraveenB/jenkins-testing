from ghost_calc_pipelines.components.de_pipeline.src import constants as constant
from ghost_calc_pipelines.components.de_pipeline.src.helper.helper import Helper
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
import json
from datetime import datetime
from urllib.parse import urlparse
import logging
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
    task_id="inference_metrics",
    requirements=["kfp==2.0.1", "protobuf==3.20.3", "kfp-pipeline-spec==0.2.2", "google-cloud-aiplatform==1.24.1",
                  "google-cloud-storage==2.10.0"],
    system_site_packages=False,
    templates_dict={"run_date": Variable.get(key="run_date"), "run_id": Variable.get(key="run_id")}
)
def inference_metrics(location_groups, **context):
    """
    Example function that will be performed in a virtual environent.

    Importing at the module level ensures that it will not attempt to import the
    library before it is installed.
    """
    from time import sleep
    import subprocess
    import json
    import logging
    from google.cloud import aiplatform
    from google.cloud import storage
    # from google_cloud_pipeline_components.v1.custom_job import create_custom_training_job_from_component
    import kfp
    import kfp.v2.dsl as dsl
    from kfp.v2.dsl import component, Output, HTML
    # import test_kflow
    # from dags.ghost_calc_pipelines.components.ml_pipeline.src import inference_kubeflow

    logging.info("Starting subprocess -- ")

    #file_location ="/home/airflow/gcs/dags/ghost_calc_pipelines/components/ml_pipeline/src/inference_kubeflow.py"
    # file_location = "/opt/airflow/dags/ghost_calc_pipelines/components/ml_pipeline/src/inference_kubeflow.py"

    import json
    import os
    from datetime import datetime
    from google.cloud import aiplatform
    # from google_cloud_pipeline_components.v1.custom_job import create_custom_training_job_from_component
    import kfp
    import kfp.v2.dsl as dsl
    from kfp.v2.dsl import component, Output, HTML

    credential_path = "/home/airflow/gcs/dags/ghost_calc_pipelines/components/de_pipeline/src/credentials.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    os.environ['GOOGLE_CLOUD_PROJECT'] = "dollar-tree-project-369709"

    # Instantiate a Google Cloud Storage client and specify required bucket and file
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('vertex-scripts')
    blob = bucket.blob('ml-scripts/inference-metrics.json')

    # Download the contents of the blob as a string and then parse it using json.loads() method
    data = json.loads(blob.download_as_string(client=None))
    BUCKET_URI = data.get("bucket_uri")
    aiplatform.init(project=data.get("project_id"), staging_bucket=data.get("bucket_uri"), location=data.get("region"))

    @component(
        output_component_file="demo_component.yaml",
        base_image="python:3.7",
        packages_to_install=["google-cloud-aiplatform==1.24.1"],
    )
    def fd_inference_metric(location_groups: list = [], config_data: dict = {}, run_date: str = None,
                            run_id:str = None) -> str:
        """
        Defining Kubeflow component

        Args:
            location_groups:
            config_data:
            run_date:

        Returns:

        """

        def get_infra_specs(location_groups: list, config_data: dict, run_date: str,run_id:str):
            """
            Defining worker specification for Vertex AI Custom Job

            Args:
                location_groups:
                config_data:
                run_date:

            Returns:

            """
            worker_pool_specs = [{
                'machine_spec': {
                    'machine_type': config_data.get("machine_type"),
                },
                'replica_count': 1,
                'container_spec': {
                    'image_uri': config_data.get("CONTAINER_TRAIN_IMAGE"),
                    "args": [
                        '--actual_file_path',
                        config_data.get("actual_file_path"),
                        '--inference_output_file_path',
                        config_data.get("inference_output_file_path") + run_id + "/prediction",
                        '--create_single_prediction_csv', config_data.get("create_single_prediction_csv"),
                        '--single_prediction_csv_output_path',
                        config_data.get("single_prediction_csv_output_path") + "/" + run_id,
                        '--metrics_output_file_path',
                        config_data.get("metrics_output_file_path")+"/"+run_id,
                        '--model_performance_report_output_file_path',
                        config_data.get("model_performance_report_output_file_path") + "/" + run_id,
                        '--dask_address', config_data.get("dask_address"),
                        '--num_workers_local_cluster', config_data.get("num_workers_local_cluster"),
                        '--num_threads_per_worker', config_data.get("num_threads_per_worker"),
                        '--memory_limit_local_worker', config_data.get("memory_limit_local_worker"),
                        '--dask_connection_timeout', config_data.get("dask_connection_timeout"),
                        '--local_dask_flag', config_data.get("local_dask_flag")
                    ]
                },
            }]
            return worker_pool_specs

        def train(job_spec, config_data=config_data):
            """
            Running Vertex AI Custom Job with defined worker specs

            Args:
                job_spec:
                config_data:

            Returns:

            """
            from google.cloud import aiplatform
            from datetime import datetime
            my_job = aiplatform.CustomJob(
                display_name="inference-metric" + "-" + datetime.now().strftime("%f"),
                worker_pool_specs=job_spec,
                staging_bucket=config_data.get("staging_bucket"),
                project=config_data.get("project_id"),
                location=config_data.get("region")
            )
            my_job.run(service_account=config_data.get("service_account"))

        specs = get_infra_specs(location_groups=location_groups, config_data=config_data, run_date=run_date,run_id=run_id)
        train(specs, config_data=config_data)

        return "success"

    PIPELINE_ROOT = "gs://{}/darshan/raw_data/machine_settings".format(BUCKET_URI)

    CPU_LIMIT = "16"  # vCPUs
    MEMORY_LIMIT = "100G"

    @dsl.pipeline(
        name="inference-metric",
        description="A simple pipeline that requests component-level machine resource",
        pipeline_root=PIPELINE_ROOT,
    )
    def pipeline(location_groups: list = [], config_data: dict = {}, run_date: str = None,run_id:str=None):
        """
        Defining Kubeflow pipeline

        Args:
            location_groups:
            config_data:
            run_date:

        Returns:

        """
        training_job_task = (
            fd_inference_metric(location_groups=location_groups, config_data=config_data, run_date=run_date,run_id=run_id)
            .set_display_name("fd-inference-metrics" + "-" + datetime.now().strftime("%f"))
            .set_cpu_limit(CPU_LIMIT)
            .set_memory_limit(MEMORY_LIMIT)
        )

    def main(location_groups: list, config_data: dict, run_date: str,run_id:str):
        """
        Compiling Kubeflow pipeline and submiting the run

        Args:
            location_groups:
            config_data:
            run_date:

        Returns:

        """
        print(" Running Kubeflow pipeline ** ")
        kfp.v2.compiler.Compiler().compile(
            pipeline_func=pipeline,
            pipeline_parameters={"location_groups": location_groups, "config_data": config_data, "run_date": run_date,
                                 "run_id":run_id},
            package_path="component_level_settings.json",
        )

        aipipeline = aiplatform.PipelineJob(
            display_name="component-level-settings",
            template_path="component_level_settings.json",
            pipeline_root=PIPELINE_ROOT,
            enable_caching=False,
        )

        aipipeline.run()

    main(location_groups=location_groups, config_data=data, run_date=context.get("templates_dict").get("run_date"),
         run_id=context.get("templates_dict").get("run_id"))
    return location_groups
