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


class DataSplitHelper(Helper):

    def __init__(self):
        super().__init__()

    def get_pyspark_data_split_args(self, location_groups) -> list:
        """

        Args:
            location_groups:

        Returns:

        """

        pyspark_args = ['--location_group_list',
                        ",".join(location_groups),
                        '--input_path',
                        self.get_env_variable("dev-data-datasplit", "input_path", "base_bucket", "base_path"),
                        '--data_split_out_path',
                        self.get_env_variable("dev-data-datasplit", "data_split_out_path", "base_bucket", "base_path"),
                        '--target_column_list',
                        ",".join(['BINARY_HL_TARGET_CLASSIFICATION', 'TARGET_REGRESSION']),
                        '--run_mode',
                        "I",
                        '--max_records_per_batch',
                        self.get_env_variable("dev-data-datasplit", "max_records_per_batch")
                        ]

        # pyspark_args_list = self.get_env_variable("dev-data-denorm", "pyspark_args")
        print("******** pyspark ***********")
        print(pyspark_args)
        return pyspark_args

    def submit_dataproc_job(self, location_groups, batch_id, context):
        batch_config = {
            "pyspark_batch": {
                "main_python_file_uri": self.get_env_variable("dev-data-datasplit", "main_python_file_uri"),
                "args": self.get_pyspark_data_split_args(location_groups),
                "python_file_uris": self.get_env_variable("dev-data-datasplit", "python_file_uris"),
            },
            "runtime_config": {
                "properties": self.get_env_variable("dev-data-datasplit", "spark_properties")
            },
            "environment_config": {
                "execution_config": {
                    "service_account": self.get_env_variable("dev-env-config", "service_account"),
                    "subnetwork_uri": self.get_env_variable("dev-env-config", "subnetwork_uri")
                },
            }
        }
        print("** batch config **")
        print(batch_config)
        run_batch = DataprocCreateBatchOperator(
            task_id="datasplit" + batch_id,
            project_id="dollar-tree-project-369709",
            region="us-west1",
            batch=batch_config,
            batch_id="datasplit-" + batch_id,
            retries=self.retry_count,
            retry_delay=self.retry_interval
        )
        return run_batch.execute(context)
