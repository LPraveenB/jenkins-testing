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


class MergeHelper(Helper):

    def __init__(self):
        super().__init__()

    def get_pyspark_validator_args(self, location_groups, component_name) -> list:
        """

        Args:
            ingested_files:
            location_groups:
            date_to_process:
            session_id:
            task_name:

        Returns:

        """

        pyspark_args = ["--source_path",
                        self.get_env_variable("dev-data-" + component_name, "dest_path", "base_bucket", "base_path"),
                        "--dest_path",
                        self.get_env_variable("dev-data-merge", component_name + "_consolidated", "base_bucket"),
                        "--load_date",
                        Variable.get(key="run_date"),
                        "--location_group_list",
                        ",".join(location_groups),
                        ]

        # pyspark_args_list = self.get_env_variable("dev-data-denorm", "pyspark_args")
        logging.info("******** pyspark ***********")
        logging.info(pyspark_args)
        return pyspark_args

    def submit_dataproc_job_old(self, batch_id, location_groups, component_name, context):
        batch_config = {
            "pyspark_batch": {
                "main_python_file_uri": self.get_env_variable("dev-data-merge", "main_python_file_uri", "script_bucket",
                                                              "script_folder"),
                "args": self.get_pyspark_validator_args(location_groups, component_name),
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
            task_id="merge" + batch_id,
            project_id="dollar-tree-project-369709",
            region="us-west1",
            batch=batch_config,
            batch_id="merge" + batch_id,
            retries=self.retry_count,
            retry_delay=self.retry_interval
        )
        return run_batch.execute(context)

    def gcs_merge_job(self, batch_id, location_groups, component_name, context) -> dict:
        """
        Check dropped files from client
        Args:
            dest_path:
            src_path:

        Returns:

        """
        run_date = Variable.get(key="run_date")
        for location_group in location_groups:
            src_path = self.get_env_variable("dev-data-" + component_name, "dest_path", "base_bucket", "base_path") \
                       + "/LOCATION_GROUP=" + location_group
            dest_path = self.get_env_variable("dev-data-merge", component_name + "_consolidated", "base_bucket") \
                        + "/LOCATION_GROUP=" + location_group + "/LOAD_DATE=" + run_date + "/"
            src_gcs_path = urlparse(src_path, allow_fragments=False)
            dest_gcs_path = urlparse(dest_path, allow_fragments=False)

            src_bucket_name, src_folder = src_gcs_path.netloc, src_gcs_path.path.lstrip("/")
            dest_bucket_name, dest_folder = dest_gcs_path.netloc, dest_gcs_path.path.lstrip("/")

            logging.info("** bucket name " + src_bucket_name)
            logging.info("** src folder " + src_folder)
            client = storage.Client()
            src_bucket = storage.Bucket(client, src_bucket_name)
            dest_bucket = storage.Bucket(client, dest_bucket_name)

            list_blobs = client.list_blobs(src_bucket_name, prefix=src_folder)

            for each_blob in list_blobs:
                src_bucket.copy_blob(each_blob, dest_bucket, dest_folder + each_blob.name.split('/')[-1])

        return batch_id
