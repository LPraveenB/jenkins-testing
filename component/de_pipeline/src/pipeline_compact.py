import os
import logging
from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from ghost_calc_pipelines.components.de_pipeline.src.helper.pipeline_helper import PipelineHelper
from ghost_calc_pipelines.components.de_pipeline.src.helper import inference_helper
from ghost_calc_pipelines.components.de_pipeline.src.helper import inference_metrics_helper
from ghost_calc_pipelines.components.de_pipeline.src import constants as constant
import json
import traceback

default_args = {
    'start_date': days_ago(1),
}

credential_path = "/home/airflow/gcs/dags/ghost_calc_pipelines/components/de_pipeline/src/credentials.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
os.environ['GOOGLE_CLOUD_PROJECT'] = "dollar-tree-project-369709"


def check_received_files():
    obj_helper = PipelineHelper()
    missing_files = obj_helper.get_missing_files()
    bash_command = obj_helper.get_bash_command(missing_files)
    return {"bash_command": bash_command, "missing_files": missing_files}


def initialize_pipeline():
    current_date_time = datetime.now()
    current_date = current_date_time.strftime("%Y%m%d")
    current_time = current_date_time.strftime("%H%M%S")
    run_id = current_date + "_" + current_time
    Variable.set(key="run_date", value=current_date)
    Variable.set(key="run_id", value=run_id)
    return run_id


@task_group(group_id='ingestion')
def fd_ingestion():
    @task
    def trigger_ingestion(**context):
        current_date_time = datetime.now()
        current_date = Variable.get(key="run_date")
        current_time = current_date_time.strftime("%H%M%S")
        # ToDo : if current_date  is null use current date from datetime
        if current_date is "None":
            current_date = current_date_time.strftime("%Y-%m-%d")
        run_id = current_date + "-" + current_time
        Variable.set(key="run_id", value=run_id)
        obj_helper = PipelineHelper()
        bash_command = check_received_files()
        obj_helper.run_ingestion(bash_command["bash_command"], context)
        print(context["ti"].xcom_pull(key="session_id", task_ids="init_ingest"))
        return bash_command["missing_files"]

    @task
    def process_ingested_data(missing_files, **context):
        obj_helper = PipelineHelper()
        context["ti"].xcom_push(key="missing_files", value=missing_files)
        return obj_helper.get_valid_files_after_ingestion()

    return process_ingested_data(trigger_ingestion())


@task
def validator(ingested_files, **context):
    """

    Args:
        ingested_files:  file path which were ingested
        missing files: list of missing files
        **context:

    Returns: will return nothing

    """
    logging.info(" ingested files list ")
    logging.info(ingested_files)
    missing_files = context["ti"].xcom_pull(key="missing_files")
    logging.info(missing_files)
    obj_helper = PipelineHelper()
    obj_helper.submit_validator_job(ingested_files, missing_files, context)


@task
def threshold(handshake_ip, **context):
    """

    Args:
        missing files: list of missing files
        **context:

    Returns:

    """
    logging.info(handshake_ip)
    missing_files = context["ti"].xcom_pull(key="missing_files")
    logging.info(missing_files)
    obj_helper = PipelineHelper()
    obj_helper.submit_thershold_job(missing_files, context)
    # return final valid files to be processed
    valid_files = obj_helper.get_valid_files_after_ingestion(process_name="preprocess")
    return valid_files


@task
def preprocess(valid_files, **context):
    obj_helper = PipelineHelper()
    obj_helper.submit_preprocess_job(valid_files, context)
    return "path_to_preprocessed_dir"


@task
def get_list_location_groups(path_to_preprocessed_dir):
    # toDo :change static path
    obj_helper = PipelineHelper()
    location_groups = obj_helper.get_location_groups()
    location_groups = obj_helper.get_location_group_in_batches(location_groups=location_groups,
                                                               component_name=constant.DENORM)
    # location_groups = [['0.0'],['1.0']]
    return location_groups


@task_group(group_id='denorm-bfs-inference')
def denorm(location_groups):
    @task
    def submit_job(location_group, **context):
        logging.info(" location group received in submit job- ")
        obj_helper = PipelineHelper()
        batch_id = obj_helper.submit_denorm_job(location_group, context)
        return location_group, batch_id

    @task
    def merge_denorm(location_group, **context):
        location_group = location_group[0]
        logging.info(location_group)
        obj_helper = PipelineHelper()
        obj_helper.denorm_merge(location_group, context)
        return location_group

    @task
    def submit_bfs_job(location_group, **context):
        print(" business_feature_store")
        obj_helper = PipelineHelper()
        batch_id = obj_helper.submit_bfs_job(location_group, context)
        return location_group, batch_id

    @task
    def merge_bfs(location_group, **context):
        location_group = location_group[0]
        logging.info(location_group)
        logging.info(location_group)
        obj_helper = PipelineHelper()
        obj_helper.bfs_merge(location_group, context)
        return location_group

    @task
    def inference(location_group, **context):
        obj_helper = PipelineHelper()
        obj_helper.trigger_inference(location_group, context)

    processed_location_group = inference(merge_bfs(submit_bfs_job(merge_denorm(submit_job(location_groups)))))
    # denorm_location_group = delete_job(submit_job(location_groups))
    logging.info(processed_location_group)
    return processed_location_group


@dag(schedule_interval=None, default_args=default_args, catchup=False)
def prod_pipeline_compact():
    """

    Returns:

    """
    valid_files = threshold(validator.expand(ingested_files=fd_ingestion()))
    location_groups = get_list_location_groups(preprocess.expand(valid_files=valid_files))
    denorm_processed_grp = denorm.expand(location_groups=location_groups)

dag = prod_pipeline_compact()
