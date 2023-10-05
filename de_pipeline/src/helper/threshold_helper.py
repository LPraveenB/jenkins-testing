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


class ThresholdHelper(Helper):

    def __init__(self):
        super().__init__()

    def get_pyspark_threshold_args(self, date_missing_files) -> list:
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

        missing_files_dict = ""
        for date in date_missing_files.items():
            missing_files_dict = str(missing_files_dict) + "\"" + date[0] + "\": \"" + str(date[1]).replace("{",
                                                                                                            "").replace(
                "}", "") \
                .replace("'", "") + "\" ,"

        pyspark_args = [
                        "--error_path",
                        self.get_env_variable("dev-data-validator", "error_path", "base_bucket", "base_path"),
                        "--date_missing_files",
                        "{"+missing_files_dict.rstrip(",")+"}"
                        ]

        # pyspark_args_list = self.get_env_variable("dev-data-denorm", "pyspark_args")
        logging.info("******** pyspark ***********")
        logging.info(pyspark_args)
        return pyspark_args

    def submit_dataproc_job(self, batch_id, missing_files, context):
        batch_config = {
            "pyspark_batch": {
                "main_python_file_uri": self.get_env_variable("dev-data-validator", "threshold_python_uri"),
                "args": self.get_pyspark_threshold_args(missing_files),
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
            task_id="threshold" + batch_id,
            project_id="dollar-tree-project-369709",
            region="us-west1",
            batch=batch_config,
            batch_id="threshold" + batch_id,
            retries=self.retry_count,
            retry_delay=self.retry_interval
        )
        return run_batch.execute(context)
