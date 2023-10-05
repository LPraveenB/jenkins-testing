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
from google.cloud import aiplatform
from random import randint


class InferenceHelper(Helper):

    def __init__(self):
        super().__init__()

    def submit_inference(self, location_groups: list, context):
        """
        Defining worker specification for Vertex AI Custom Job

        Args:
            context:
            location_groups:
            config_data:
            run_date:

        Returns:

        """
        project_id = self.get_env_variable("dev-data-inference", "project_id")
        bucket_uri = self.get_env_variable("dev-data-inference", "bucket_uri")
        region = self.get_env_variable("dev-data-inference", "region")
        run_date = Variable.get(key="run_date")

        aiplatform.init(project=project_id, staging_bucket=bucket_uri, location=region)

        worker_pool_specs = [{
            'machine_spec': {
                'machine_type': self.get_env_variable("dev-data-inference", "machine_type"),
            },
            'replica_count': 1,
            'container_spec': {
                'image_uri': self.get_env_variable("dev-data-inference", "CONTAINER_TRAIN_IMAGE"),
                "args": [
                    '--source_path_model_s1',
                    self.get_env_variable("dev-data-inference", "source_path_model_s1"),
                    '--source_path_model_s2',
                    self.get_env_variable("dev-data-inference", "source_path_model_s2"),
                    '--source_path_model_s3',
                    self.get_env_variable("dev-data-inference", "source_path_model_s3"),
                    '--source_path_model_s4',
                    self.get_env_variable("dev-data-inference", "source_path_model_s4"),
                    '--decision_threshold_step_1',
                    self.get_env_variable("dev-data-inference", "decision_threshold_step_1"),
                    '--decision_threshold_step_2',
                    self.get_env_variable("dev-data-inference", "decision_threshold_step_1"),
                    '--business_feature_store_base_path',
                    self.get_env_variable("dev-data-inference", "business_feature_store_base_path"),
                    '--output_path', self.get_env_variable("dev-data-inference", "output_path") + Variable.get(
                        key="run_id") + "/prediction",
                    '--location_group_list', " ".join(location_groups),
                    '--load_date', run_date,
                    '--dask_address', self.get_env_variable("dev-data-inference", "dask_address"),
                    '--num_workers_local_cluster',
                    self.get_env_variable("dev-data-inference", "num_workers_local_cluster"),
                    '--num_threads_per_worker', self.get_env_variable("dev-data-inference", "num_threads_per_worker"),
                    '--memory_limit_local_worker',
                    self.get_env_variable("dev-data-inference", "num_threads_per_worker"),
                    '--dask_connection_timeout', self.get_env_variable("dev-data-inference", "dask_connection_timeout"),
                    '--local_dask_flag', self.get_env_variable("dev-data-inference", "local_dask_flag")
                ]
            },
        }]

        my_job = aiplatform.CustomJob(
            display_name="inference" + "-" + datetime.now().strftime("%f") + "-" + str(randint(1, 100)),
            worker_pool_specs=worker_pool_specs,
            staging_bucket=self.get_env_variable("dev-data-inference", "staging_bucket"),
            project=self.get_env_variable("dev-data-inference", "project_id"),
            location=self.get_env_variable("dev-data-inference", "region")
        )

        my_job.run(service_account=self.get_env_variable("dev-data-inference", "service_account"))

        return location_groups
