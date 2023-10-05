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
from datetime import datetime


class ValidatorHelper(Helper):

    def __init__(self):
        super().__init__()

    def get_pyspark_validator_args(self, ingested_files,missing_files,batch_id) -> list:
        """

        Args:
            missing_files:
            ingested_files:
            location_groups:
            date_to_process:
            session_id:
            task_name:

        Returns:

        """

        pyspark_args = ["--input_path",
                        ",".join(ingested_files),
                        "--error_path",
                        self.get_env_variable("dev-data-validator", "error_path", "base_bucket", "base_path"),
                        "--report_path",
                        self.get_env_variable("dev-data-validator", "report_path", "base_bucket", "base_path"),
                        "--json_path",
                        self.get_env_variable("dev-data-validator", "json_path", "base_bucket", "base_path"),
                        "--date_missing_files",
                        str(missing_files),
                        "--batch_id",
                        Variable.get(key="run_date").replace("_", "-") + "-" + datetime.now().strftime("%f")
                        ]

        # pyspark_args_list = self.get_env_variable("dev-data-denorm", "pyspark_args")
        logging.info("******** pyspark ***********")
        logging.info(pyspark_args)
        return pyspark_args

    def submit_dataproc_job(self, batch_id, ingested_files, missing_files, context):
        batch_config = {
            "pyspark_batch": {
                "main_python_file_uri": self.get_env_variable("dev-data-validator", "main_python_file_uri"),
                "args": self.get_pyspark_validator_args(ingested_files,missing_files,batch_id),
                "python_file_uris": self.get_env_variable("dev-data-validator", "python_file_uris")
            },
            "runtime_config": {
                "properties": self.get_env_variable("dev-data-validator", "spark_properties")
            },
            "environment_config": {
                "execution_config": {
                    "service_account": self.get_env_variable("dev-env-config", "service_account"),
                    "subnetwork_uri": self.get_env_variable("dev-env-config", "subnetwork_uri")
                },
            }
        }

        logging.info(" printing batch config ********* ")
        logging.info(batch_config)

        run_batch = DataprocCreateBatchOperator(
            task_id="validator" + batch_id,
            project_id="dollar-tree-project-369709",
            region="us-west1",
            batch=batch_config,
            batch_id="validator" + batch_id,
            retries=self.retry_count,
            retry_delay=self.retry_interval
        )
        return run_batch.execute(context)