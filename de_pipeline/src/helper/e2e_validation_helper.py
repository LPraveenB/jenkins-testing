from ghost_calc_pipelines.components.de_pipeline.src import constants as constant
from ghost_calc_pipelines.components.de_pipeline.src.helper.helper import Helper
import json
from urllib.parse import urlparse
import logging
import math
from google.cloud import storage
import re
from google.cloud import dataproc_v1 as dataproc
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator
)
from airflow.models import Variable


class E2EHelper(Helper):

    def __init__(self):
        super().__init__()
        self.retry_count = 1

    def get_e2e_validator_args(self) -> list:
        """

        Args:


        Returns:

        """

        pyspark_args = ['--bucket_name',
                        self.get_env_variable("dev-e2e-validator", "base_bucket", "base_path"),
                        '--input_path',
                        self.get_env_variable("dev-e2e-validator", "input_path", "base_path"),
                        '--error_path',
                        self.get_env_variable("dev-e2e-validator", "error_path", "base_path"),

                        ]

        return pyspark_args

    def submit_dataproc_job(self, location_group, batch_id, context):

        batch_config = {
            "pyspark_batch": {
                "main_python_file_uri": self.get_env_variable("dev-e2e-validator", "main_python_file_uri", "script_bucket", "script_folder"),
                "args": self.get_e2e_validator_args(),
                "python_file_uris": [self.get_env_variable("dev-e2e-validator", "python_file_uris")]
            },
            "runtime_config": {
                "properties": self.get_env_variable("dev-e2e-validator", "spark_properties")
            },
            "environment_config": {
                "execution_config": {
                    "service_account": self.get_env_variable("dev-env-config", "service_account"),
                    "subnetwork_uri": self.get_env_variable("dev-env-config", "subnetwork_uri")
                },
            }
        }

        print(" printing batch config ********* ")
        print(batch_config)

        run_batch = DataprocCreateBatchOperator(
            task_id="e2e-validator" + batch_id,
            project_id="dollar-tree-project-369709",
            region="us-west1",
            batch=batch_config,
            batch_id="e2e-validator-" + batch_id,
            retries=self.retry_count,
            retry_delay=self.retry_interval
        )
        return run_batch.execute(context)
