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

"""credential_path = "/home/airflow/gcs/dags/ghost_calc_pipelines/components/de_pipeline/src/credentials.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
os.environ['GOOGLE_CLOUD_PROJECT'] = "dollar-tree-project-369709"""


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


@task_group(group_id='fd_ingestion')
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
    location_groups = [['0.0']]
    return location_groups


@task_group
def denorm(location_groups):
    @task
    def submit_job(location_group, **context):
        logging.info(" location group received in submit job- ")
        obj_helper = PipelineHelper()
        batch_id = obj_helper.submit_denorm_job(location_group, context)
        return location_group, batch_id

    @task
    def delete_job(submit_batch_details, **context):
        batch_id = submit_batch_details[1]
        location_groups = submit_batch_details[0]
        obj_helper = PipelineHelper()
        obj_helper.delete_batch(constant.DENORM, batch_id, context)
        return location_groups

    denorm_location_group = delete_job(submit_job(location_groups))
    # denorm_location_group = delete_job(submit_job(location_groups))
    logging.info(denorm_location_group)
    return denorm_location_group

@task
def start_audit_apply(location_groups):
    logging.info("waiting for denorm completion")
    logging.info(location_groups)
    return location_groups

@task_group(group_id='Audit_Apply')
def audit_apply(location_groups):
    @task
    def submit_job(location_group, **context):
        obj_helper = PipelineHelper()
        batch_id = obj_helper.submit_audit_data_apply(location_group, context)
        return location_group, batch_id

    @task
    def delete_job(submit_batch_details, **context):
        batch_id = submit_batch_details[1]
        location_group = submit_batch_details[0]
        obj_helper = PipelineHelper()
        obj_helper.delete_batch(constant.AUDIT_DATA, batch_id, context)
        return location_group

    audit_location_grp = delete_job(submit_job(location_groups))
    logging.info(audit_location_grp)
    return audit_location_grp

@task
def start_e2e_validation(location_groups):
    logging.info("waiting for denorm completion")
    logging.info(location_groups)
    return location_groups


@task_group(group_id='E2E_Validation')
def e2e_validation(location_groups):
    @task
    def submit_job(location_group, **context):
        logging.info(" location group received in submit job- ")
        obj_helper = PipelineHelper()
        batch_id = obj_helper.submit_e2e_validator_job(location_group, context)
        return location_group, batch_id

    @task
    def delete_job(submit_batch_details, **context):
        batch_id = submit_batch_details[1]
        location_groups = submit_batch_details[0]
        obj_helper = PipelineHelper()
        obj_helper.delete_batch(constant.E2E_VALIDATOR, batch_id, context)
        return location_groups


    e2e_location_groups = delete_job(submit_job(location_groups))
    # denorm_location_group = delete_job(submit_job(location_groups))
    logging.info(e2e_location_groups)
    return e2e_location_groups


@task
def merge_denorm(location_groups, **context):
    logging.info("merging denorm")
    obj_helper = PipelineHelper()
    obj_helper.denorm_merge(location_groups, context)
    logging.info("-- merging --")
    logging.info(location_groups)
    return location_groups


@task
def start_bfs(location_groups):
    logging.info("waiting for BFS completion")
    logging.info(location_groups)
    obj_helper = PipelineHelper()
    location_groups = obj_helper.get_location_group_in_batches(location_groups=location_groups,
                                                               component_name=constant.BUSINESS_FS)
    return location_groups


@task_group
def business_fs(location_groups):
    @task
    def submit_job(location_group, **context):
        print(" business_feature_store")
        obj_helper = PipelineHelper()
        batch_id = obj_helper.submit_bfs_job(location_group, context)
        return location_group, batch_id

    @task
    def delete_job(submit_batch_details, **context):
        batch_id = submit_batch_details[1]
        location_groups = submit_batch_details[0]
        obj_helper = PipelineHelper()
        obj_helper.delete_batch(constant.BUSINESS_FS, batch_id, context)
        return location_groups

    @task
    def merge_bfs(location_groups, **context):
        logging.info("merging bfs")
        obj_helper = PipelineHelper()
        obj_helper.bfs_merge(location_groups, context)
        logging.info("-- merging --")
        logging.info(location_groups)
        return location_groups

    bfs_location_group = merge_bfs(delete_job(submit_job(location_groups)))
    # bfs_location_group = (delete_job(submit_job(location_groups)))
    logging.info(bfs_location_group)
    return bfs_location_group


@task
def start_inference(location_groups, **context):
    logging.info("waiting for denorm completion")
    logging.info(location_groups)
    obj_helper = PipelineHelper()
    #inference_location_groups = obj_helper.inference_pipeline_sizing(location_groups)
    inference_location_groups = obj_helper.get_location_group_in_batches(location_groups=location_groups,
                                                               component_name=constant.INFERENCE)
    return inference_location_groups


@task
def inference(location_groups, **context):
    obj_helper = PipelineHelper()
    obj_helper.trigger_inference(location_groups, context)


@task
def start_inference_metric(location_groups, **context):
    logging.info("waiting for denorm completion")
    logging.info(location_groups)
    return location_groups


@dag(schedule_interval=None, default_args=default_args, catchup=False)
def prod_pipeline_v1():
    """

    Returns:

    """
    valid_files = threshold(validator.expand(ingested_files=fd_ingestion()))
    location_groups = get_list_location_groups(preprocess.expand(valid_files=valid_files))
    denorm_processed_grp = start_audit_apply(denorm.expand(location_groups=location_groups))
    audit_processed_grps = start_e2e_validation(audit_apply.expand(location_groups=denorm_processed_grp))
    e2e_processed_grps = start_bfs(e2e_validation.expand(location_groups=audit_processed_grps))
    # bfs_location_grp = start_inference(business_fs.expand(location_groups=e2e_processed_grps))
    # inf_location_grp = end_pipeline(inference.expand(location_groups=bfs_location_grp))
    # inference_metrics_helper.inference_metrics(inf_location_grp)


dag = prod_pipeline_v1()
