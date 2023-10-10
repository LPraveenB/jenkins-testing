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


class BfsHelper(Helper):

    def __init__(self):
        super().__init__()

    def get_pyspark_denorm_args(self, location_groups) -> list:
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
                        self.get_env_variable("dev-data-bfs", "mapping_path"),
                        '--denorm_src_path',
                        self.get_env_variable("dev-data-bfs", "denorm_src_path","base_bucket"),
                        '--meta_src_path',
                        self.get_env_variable("dev-data-bfs", "meta_src_path","base_bucket"),
                        '--dest_path',
                        self.get_env_variable("dev-data-bfs", "dest_path","base_bucket","base_path"),
                        '--historical_mode',
                        self.get_env_variable("dev-env-config", "historical_mode"),
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
                "main_python_file_uri": self.get_env_variable("dev-data-bfs", "main_python_file_uri"),
                "args": self.get_pyspark_denorm_args(location_groups),
                "python_file_uris": self.get_env_variable("dev-data-bfs", "python_file_uris"),
                "file_uris": self.get_env_variable("dev-data-bfs", "file_uris")
            },
            "runtime_config": {
                "properties": self.get_env_variable("dev-data-bfs", "spark_properties")
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
            task_id="bfs" + batch_id,
            project_id="dollar-tree-project-369709",
            region="us-west1",
            batch=batch_config,
            batch_id="bfs" + batch_id,
            retries=self.retry_count,
            retry_delay=self.retry_interval
        )
        return run_batch.execute(context)
