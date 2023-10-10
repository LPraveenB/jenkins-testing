from ghost_calc_pipelines.components.de_pipeline.src import constants as constant
from ghost_calc_pipelines.components.de_pipeline.src.helper.helper import Helper
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
import json
from urllib.parse import urlparse
import logging
from datetime import datetime
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


@task.virtualenv(
    task_id="inference",
    requirements=["kfp==2.0.1", "protobuf==3.20.3", "kfp-pipeline-spec==0.2.2", "google-cloud-aiplatform==1.24.1",
                  "google-cloud-storage==2.10.0"],
    system_site_packages=False,
    op_kwargs={"run_date": Variable.get(key="run_date"), "run_id": Variable.get(key="run_id")}
)
def training(location_groups, **context):
    """
    Example function that will be performed in a virtual environent.

    Importing at the module level ensures that it will not attempt to import the
    library before it is installed.
    """
    from time import sleep
    import subprocess
    import json

    from google.cloud import aiplatform
    from google.cloud import storage
    # from google_cloud_pipeline_components.v1.custom_job import create_custom_training_job_from_component
    import kfp
    import kfp.v2.dsl as dsl
    from kfp.v2.dsl import component, Output, HTML

    import json
    import os
    from datetime import datetime
    from google.cloud import aiplatform
    from random import randint
    # from google_cloud_pipeline_components.v1.custom_job import create_custom_training_job_from_component
    import kfp
    import kfp.v2.dsl as dsl
    from kfp.v2.dsl import component, Output, HTML

    def main(location_groups: list, config_data: dict, run_date: str, run_id: str):
        """
        Compiling Kubeflow pipeline and submiting the run

        Args:
            run_id:
            location_groups:
            config_data:
            run_date:

        Returns:

        """
        print(" Running Kubeflow pipeline ** " + " ".join(location_groups))
        kfp.v2.compiler.Compiler().compile(
            pipeline_func=pipeline,
            pipeline_parameters={"location_groups": location_groups, "config_data": config_data, "run_date": run_date,
                                 "run_id": run_id},
            package_path="component_level_settings.json",
        )

        aipipeline = aiplatform.PipelineJob(
            display_name="component-level-settings",
            template_path="component_level_settings.json",
            pipeline_root=PIPELINE_ROOT,
            enable_caching=False,
        )

        aipipeline.run()
        return location_groups

    main(location_groups=location_groups, config_data=data, run_date=context.get("run_date"),
         run_id=context.get("run_id"))
    return location_groups
