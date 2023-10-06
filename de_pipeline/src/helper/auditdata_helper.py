from ghost_calc_pipelines.components.de_pipeline.src import constants as constant
from ghost_calc_pipelines.components.de_pipeline.src.helper.helper import Helper
import json
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


class AuditDataHelper(Helper):

    def __init__(self):
        super().__init__()

    def get_pyspark_audiit_args(self, location_groups) -> list:
        """

        Args:
            location_groups:
            date_to_process:
            session_id:
            task_name:

        Returns:

        """
        pyspark_args = ['--location_groups',
                        ",".join(location_groups),
                        '--mapping_path',
                        self.get_env_variable("dev-data-audit", "mapping_path", "script_bucket","script_folder"),
                        '--denorm_src_path',
                        self.get_env_variable("dev-data-audit", "denorm_src_path", "base_bucket", "base_path"),
                        '--dest_path',
                        self.get_env_variable("dev-data-audit", "dest_path", "base_bucket", 'base_path'),
                        '--historical_mode',
                        self.get_env_variable("dev-data-audit", "historical_mode"),
                        '--current_date_str',
                        Variable.get(key="run_date"),
                        ]

        # pyspark_args_list = self.get_env_variable("dev-data-denorm", "pyspark_args")
        print("******** pyspark ***********")
        print(pyspark_args)
        return pyspark_args

    def submit_dataproc_job(self, location_groups, batch_id, context):
        batch_config = {
            "pyspark_batch": {
                "main_python_file_uri": self.get_env_variable("dev-data-audit", "main_python_file_uri", "script_bucket","script_folder"),
                "args": self.get_pyspark_audiit_args(location_groups)
            },
            "runtime_config": {
                "properties": self.get_env_variable("dev-data-audit", "spark_properties")
            },
            "environment_config": {
                "execution_config": {
                    "service_account": self.get_env_variable("dev-env-config", "service_account"),
                    "subnetwork_uri": self.get_env_variable("dev-env-config", "subnetwork_uri")
                },
            }
        }

        run_batch = DataprocCreateBatchOperator(
            task_id="audit" + batch_id,
            project_id="dollar-tree-project-369709",
            region="us-west1",
            batch=batch_config,
            batch_id="audit" + batch_id,
            retries=self.retry_count,
            retry_delay=self.retry_interval
        )
        return run_batch.execute(context)