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


@task
def start(path_to_preprocessed_dir):
    # toDo :change static path
    obj_helper = PipelineHelper()
    location_groups = obj_helper.get_location_groups(data_path=path_to_preprocessed_dir)
    # location_groups = [['0.0'],['1.0']]
    return location_groups


@task_group
def generate_audit_data(client_drop_path):
    @task
    def submit_job(client_drop_path, **context):
        obj_helper = PipelineHelper()
        return client_drop_path, 1

    @task
    def delete_job(submit_batch_details, **context):
        batch_id = submit_batch_details[1]
        client_drop_path = submit_batch_details[0]
        obj_helper = PipelineHelper()
        return client_drop_path

    @task
    def merge_audit(client_drop_path, **context):
        logging.info("merging audit")
        obj_helper = PipelineHelper()
        merge_path = ""
        return "gs://extracted-bucket-dollar-tree/Raghav/test_files/denorm_with_audit_all_loc_V1/LOCATION_GROUP="

    merge_path = merge_audit(delete_job(submit_job(client_drop_path)))
    # denorm_location_group = delete_job(submit_job(location_groups))
    logging.info(merge_path)
    return merge_path


@task
def start_business_metrics(audit_path,location_groups,**context):
    logging.info("waiting for audit data generation")
    logging.info(audit_path)
    obj_helper = PipelineHelper()
    obj_helper.trigger_inference_metric(location_groups, context)
    return audit_path


@task_group
def generate_business_metrics(audit_path):
    @task
    def submit_job(audit_path, **context):
        print(" generate_business_metrics")
        obj_helper = PipelineHelper()
        business_metric_path = ""
        submit_batch_details = ["batch_id", "business_metric_path"]
        return submit_batch_details

    @task
    def delete_job(submit_batch_details, **context):
        batch_id = submit_batch_details[1]
        business_metric_path = submit_batch_details[0]
        obj_helper = PipelineHelper()

        return "business_metric_path"

    business_metric_path = delete_job(submit_job(audit_path))
    # bfs_location_group = (delete_job(submit_job(location_groups)))
    return business_metric_path


@task_group
def data_split(location_groups):
    @task
    def submit_job(location_group, **context):
        print(" business_feature_store")
        obj_helper = PipelineHelper()
        batch_id = obj_helper.submit_datasplit_job(location_group, context)
        return location_group, batch_id

    @task
    def delete_job(submit_batch_details, **context):
        batch_id = submit_batch_details[1]
        location_groups = submit_batch_details[0]
        obj_helper = PipelineHelper()
        obj_helper.delete_batch(constant.DATA_SPLIT, batch_id, context)
        return location_groups

    @task
    def merge(location_groups, **context):
        logging.info("merging data split")
        obj_helper = PipelineHelper()
        obj_helper.data_split_merge(location_groups, context)
        logging.info("-- merging --")
        logging.info(location_groups)
        return location_groups

    datasplit_location_group = merge(delete_job(submit_job(location_groups)))
    logging.info(datasplit_location_group)
    return datasplit_location_group


@task
def switch_decision_component(business_metric_path, **context):
    logging.info("waiting for switch decision component")
    logging.info(business_metric_path)
    obj_helper = PipelineHelper()
    decision = true
    return decision


@task.branch
def switch(decision, **context):
    logging.info("waiting for switching decision")
    if decision:
        print("switch")
        return "switch"
    else:
        print("end")
        return "end"


@task
def end_switch(switch_output, **context):
    logging.info(switch_output)


@task
def start_inference_metric(location_groups, **context):
    logging.info("waiting for denorm completion")
    logging.info(location_groups)
    return location_groups

@task
def inference_metric(location_groups,**context):
    obj_helper = PipelineHelper()
    obj_helper.inference(location_groups,context)


@dag(schedule_interval=None, default_args=default_args, catchup=False)
def prod_pipeline_switch():
    """

    Returns:

    """
    audit_path = generate_audit_data("client_data_path")
    audit_location_groups = start(audit_path)
    data_split_location_groups = data_split.expand(location_groups=audit_location_groups)

    inference_metrics = start_business_metrics(audit_path,audit_location_groups)
    boolean_decision = switch_decision_component(inference_metrics)
    switch(boolean_decision)


dag = prod_pipeline_switch()