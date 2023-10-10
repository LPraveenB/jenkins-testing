from ghost_calc_pipelines.components.de_pipeline.src import constants as constant
from ghost_calc_pipelines.components.de_pipeline.src.helper.helper import Helper
from airflow.operators.bash import BashOperator
import json
from urllib.parse import urlparse
import logging
from google.cloud import storage
import re
import os
import math


class IngestionHelper(Helper):

    def __init__(self):
        super().__init__()

    def get_dropped_files(self, src_path: str, dest_path: str) -> dict:
        """
        Check dropped files from client
        Args:
            dest_path:
            src_path:

        Returns:

        """
        src_path = self.get_env_variable("dev-data-ingestion", "drop_path", "base_bucket", "base_path")
        dest_path = self.get_env_variable("dev-data-ingestion", "load_path", "base_bucket", "base_path")
        src_gcs_path = urlparse(src_path, allow_fragments=False)
        dest_gcs_path = urlparse(dest_path, allow_fragments=False)

        src_bucket_name, src_folder = src_gcs_path.netloc, src_gcs_path.path.lstrip("/")
        dest_bucket_name, dest_folder = dest_gcs_path.netloc, dest_gcs_path.path.lstrip("/")

        logging.info("** bucket name " + src_bucket_name)
        logging.info("** src folder " + src_folder)
        client = storage.Client()
        client = storage.Client()
        src_bucket = storage.Bucket(client, src_bucket_name)
        dest_bucket = storage.Bucket(client, dest_bucket_name)

        received_dict = {}
        list_blobs = client.list_blobs(src_bucket_name, prefix=src_folder)
        print(" listing *********")
        # list_blobs = ["FD_CARRIER_DETAILS-20230609.csv","FD_STORE_INV-20230607.csv"]
        for each_blob in list_blobs:
            file_params = re.split(r'-|\.', each_blob.name.replace(src_gcs_path.path[1:], ""))
            logging.info(file_params)
            if len(file_params) > 1:
                if file_params[1] in received_dict.keys():
                    files_received = received_dict[file_params[1]]
                    received_dict[file_params[1]] = file_params[0].replace("/", "") + "," + files_received
                else:
                    received_dict[file_params[1]] = file_params[0].replace("/", "")
            src_bucket.copy_blob(each_blob, dest_bucket, dest_folder + each_blob.name.split('/')[-1])
            logging.info(" - get completed ")
        logging.info(received_dict)
        return received_dict

    def get_missing_files(self):
        """

        caller method - to get dropped files and generate dict with key as date and value as
        comma seperated missing files
        Returns:

        """
        logging.info(" printing work directory ")
        f = open(constant.COMPONENT_PROP + "dev-data-ingestion" + ".json", )
        config_params = json.load(f)
        mandatory_file_list = config_params['required_files']
        special_files = config_params['special_files']
        received_dict = self.get_dropped_files(
            self.get_env_variable("dev-data-ingestion", "drop_path", "base_bucket", "base_path"),
            self.get_env_variable("dev-data-ingestion", "success_path", "base_bucket", "base_path"))

        missing_files = self.get_files_missing_in_dates(mandatory_file_list, special_files, received_dict)
        return missing_files

    def get_files_missing_in_dates(self, mandatory_file_list: list, special_files_list: list, received_files: dict):
        """
        compare with expected files
        Args:
            mandatory_file_list:
            special_files_list:
            received_files:

        Returns:

        """
        missing_files = dict()
        for key_date in received_files.keys():
            # list down the received files
            received_file_list = received_files[key_date].split(",")
            received_file_set = set(received_file_list)
            mandatory_file_set = set(mandatory_file_list)
            # check for missing files
            missing_mandatory_set = mandatory_file_set.difference(received_file_set)
            if missing_mandatory_set == set():
                missing_files[key_date] = "None"
            else:
                missing_files[key_date] = ','.join(missing_mandatory_set)
        logging.info("missing files = ")
        logging.info(missing_files)
        return missing_files

    def compose_bash_command(self, date_missing_files: dict) -> str:
        """

        Args:
            date_missing_files:

        Returns:

        """
        bash_command = ""
        # sub proc , run and task id ,current_time are dummy - as no more logging is needed as suggested by ram
        input_path = self.get_env_variable("dev-data-ingestion", "drop_path", "base_bucket",
                                           "base_path")  # append with date passed in date_missing_files ,  to access the files (files from drop bucket will be moved to load bucket by pipeline)
        error_path = self.get_env_variable("dev-data-ingestion", "error_path", "base_bucket",
                                           "base_path")  # append with date passed in date_missing_files ,  to access the files ( data ingestion component will be moving files from drop bucket to error bucket based on criteria
        output_path = self.get_env_variable("dev-data-ingestion", "success_path", "base_bucket",
                                            "base_path")  # append with date passed in date_missing_files ,  to access the files ( data ingestion component will be moving files from drop bucket to success bucket based on criteria

        logging.info("--- get working directory ")
        py_file_path = self.get_env_variable("dev-data-ingestion", "py_file_path")

        logging.info(" --- len of date missing ----")
        logging.debug(len(list(date_missing_files.keys())))
        missing_files_dict = ""
        for date in date_missing_files.items():
            logging.info(date)
            missing_files_dict = str(missing_files_dict) + "\"" + date[0] + "\": \"" + str(date[1]).replace("{",
                                                                                                            "").replace(
                "}", "") \
                .replace("'", "") + "\" ,"

        missing_files_dict = "{" + missing_files_dict[0:-1] + "}"
        logging.info("data too process ---------- " + missing_files_dict)
        if len(list(date_missing_files.keys())) > 0:
            bash_command = f"python " + py_file_path + \
                           " --input_path " + input_path + " --error_path " + error_path + \
                           " --log_path gs://pipeline_audit/task-details/log/task_details_tbl_new.csv " \
                           "--output_path " + output_path + \
                           " --date_missing_files '" + missing_files_dict + "'"

        return bash_command

    def run_ingestion(self, bash_command, context):
        de_task = BashOperator(
            task_id="run_ingestion",
            bash_command=bash_command,
            do_xcom_push=True,
            retries=self.retry_count,
            retry_delay=self.retry_interval
        )
        return de_task.execute(context)

    def list_files(self, drop_path):
        gcs_path = urlparse(drop_path, allow_fragments=False)
        bucket_name, filename = gcs_path.netloc, gcs_path.path.lstrip("/")
        print('bucket_name', bucket_name)
        print('filename', filename)
        client = storage.Client()
        return list(client.list_blobs(bucket_name, prefix=filename))

    def get_valid_files_after_ingestion(self, process_name: str):
        valid_files = []
        src_path = self.get_env_variable("dev-data-ingestion", "success_path", "base_bucket",
                                         "base_path")

        drop_files = self.list_files(src_path)
        for each_blob in drop_files:
            print('each_blob', each_blob)
            gcs_file_name = each_blob.name.split('/')[-1]
            print('gcs_file_name', gcs_file_name)
            if gcs_file_name.strip() != '':
                valid_files.append('gs://' + each_blob.bucket.name + '/' + each_blob.name)

        if process_name == "validator":
            parallel_cluster = int(self.get_env_variable("dev-data-ingestion", "parallel_cluster"))
            chunk_size = math.ceil(len(valid_files) / parallel_cluster)
            print(" -- chunk size ",chunk_size)
            print(" -- no of valid files ", len(valid_files))
            return [valid_files[x:x + chunk_size] for x in range(0, len(valid_files), chunk_size)]
        else:
            # passing the valid files on table level
            result_map = {}
            for item in valid_files:
                # Split the value using "/"
                split_values = item.split("/")
                # Extract the value at index -2
                key = split_values[-2]
                # Add the item to the corresponding list in the dictionary
                if key not in result_map:
                    result_map[key] = []
                result_map[key].append(item)
            resultList = [(key, value) for key, value in result_map.items()]
            return resultList
