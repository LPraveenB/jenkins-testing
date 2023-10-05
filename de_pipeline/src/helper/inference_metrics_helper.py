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
from random import randint
from google.cloud import aiplatform


class InferenceMetricsHelper(Helper):

    def __init__(self):
        super().__init__()

    def submit_inference_metrics(self, location_groups: list, context):
        """
        Defining worker specification for Vertex AI Custom Job

        Args:
            context:
            location_groups:
            config_data:
            run_date:

        Returns:

        """

        project_id = self.get_env_variable("dev-data-inference-metrics", "project_id")
        bucket_uri = self.get_env_variable("dev-data-inference-metrics", "bucket_uri")
        region = self.get_env_variable("dev-data-inference-metrics", "region")
        run_date = Variable.get(key="run_date")
        run_id = Variable.get(key="run_id")

        aiplatform.init(project=project_id, staging_bucket=bucket_uri, location=region)

        worker_pool_specs = [{
            'machine_spec': {
                'machine_type': self.get_env_variable("dev-data-inference-metrics", "machine_type"),
            },
            'replica_count': 1,
            'container_spec': {
                'image_uri': self.get_env_variable("dev-data-inference-metrics", "CONTAINER_TRAIN_IMAGE"),
                "args": [
                    '--actual_file_path',
                    self.get_env_variable("dev-data-inference-metrics", "actual_file_path", "base_bucket"),
                    '--inference_output_file_path',
                    self.get_env_variable("dev-data-inference-metrics",
                                          "inference_output_file_path", "base_bucket", "base_path"),
                    '--create_single_prediction_csv', self.get_env_variable("dev-data-inference-metrics",
                                                                            "create_single_prediction_csv"),
                    '--single_prediction_csv_output_path',
                    self.get_env_variable("dev-data-inference-metrics",
                                          "single_prediction_csv_output_path", "base_bucket", "base_path"),
                    '--metrics_output_file_path',
                    self.get_env_variable("dev-data-inference-metrics", "metrics_output_file_path", "base_bucket",
                                          "base_path"),
                    '--model_performance_report_output_file_path',
                    self.get_env_variable("dev-data-inference-metrics",
                                          "model_performance_report_output_file_path", "base_bucket", "base_path"),
                    '--dask_address', self.get_env_variable("dev-data-inference-metrics", "dask_address"),
                    '--num_workers_local_cluster',
                    self.get_env_variable("dev-data-inference-metrics", "num_workers_local_cluster"),
                    '--num_threads_per_worker',
                    self.get_env_variable("dev-data-inference-metrics", "num_threads_per_worker"),
                    '--memory_limit_local_worker',
                    self.get_env_variable("dev-data-inference-metrics", "memory_limit_local_worker"),
                    '--dask_connection_timeout',
                    self.get_env_variable("dev-data-inference-metrics", "dask_connection_timeout"),
                    '--local_dask_flag', self.get_env_variable("dev-data-inference-metrics", "local_dask_flag")
                ]
            },
        }]

        my_job = aiplatform.CustomJob(
            display_name="inference-metric" + "-" + datetime.now().strftime("%f"),
            worker_pool_specs=worker_pool_specs,
            staging_bucket=self.get_env_variable("dev-data-inference-metrics", "staging_bucket"),
            project=self.get_env_variable("dev-data-inference-metrics", "project_id"),
            location=self.get_env_variable("dev-data-inference-metrics", "region")
        )
        my_job.run(service_account=self.get_env_variable("dev-data-inference", "service_account"))

        return location_groups
