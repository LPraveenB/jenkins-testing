import json
from jinja2 import Template
from datetime import datetime, timedelta
from airflow.models import Variable
from ghost_calc_pipelines.components.de_pipeline.src import constants as constant
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator
)


class Helper:

    def __init__(self):
        self.retry_interval = timedelta(seconds=30)
        self.retry_count = 3

    def get_env_variable(self, component: str, name: str, *args):
        """
        Method is intended to read / render variables from json

        Args:
            component:
            name:
            *args:

        Returns:

        """
        f = open(constant.COMPONENT_PROP + component + ".json")
        config_params = json.load(f)
        if len(args) > 0:
            template = config_params.get(name)
            data = dict()
            for data_key in args:
                if "path" in data_key:
                    data['run_date'] = Variable.get(key="run_id")
                data[data_key] = config_params.get(data_key)

            j2_template = Template(template)

            return j2_template.render(data)
        else:
            return config_params.get(name)

    def delete_dataproc_job(self, component_name, batch_id,context):
        delete_batch_task = DataprocDeleteBatchOperator(
            task_id="delete_" + component_name + batch_id,
            batch_id=component_name + batch_id,
            project_id=self.get_env_variable("dev-env-config", "project_id"),
            region=self.get_env_variable("dev-env-config", "region")
        )
        return delete_batch_task.execute(context)
