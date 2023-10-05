from ghost_calc_pipelines.components.de_pipeline.src.helper.ingestion_helper import IngestionHelper
from ghost_calc_pipelines.components.de_pipeline.src.helper.denorm_helper import DenormHelper
from ghost_calc_pipelines.components.de_pipeline.src.helper.validator_helper import ValidatorHelper
from ghost_calc_pipelines.components.de_pipeline.src.helper.threshold_helper import ThresholdHelper
from ghost_calc_pipelines.components.de_pipeline.src.helper.preprocess_helper import PreprocessHelper
from ghost_calc_pipelines.components.de_pipeline.src.helper.e2e_validation_helper import E2EHelper
from ghost_calc_pipelines.components.de_pipeline.src.helper.merge_helper import MergeHelper
# from ghost_calc_pipelines.components.de_pipeline.src.helper.inference_helper import InferenceHelper
from ghost_calc_pipelines.components.de_pipeline.src.helper.bfs_helper import BfsHelper
from ghost_calc_pipelines.components.de_pipeline.src import constants as constant
from airflow.models import Variable
from datetime import datetime
import logging


class PipelineHelper():

    def __init__(self):
        self.ingestion = IngestionHelper()
        self.denorm = DenormHelper()
        self.validate = ValidatorHelper()
        self.preprocess = PreprocessHelper()
        self.e2e = E2EHelper()
        self.merge = MergeHelper()
        self.threshold = ThresholdHelper()
        # self.inference = InferenceHelper()
        self.bfs = BfsHelper()

    def get_missing_files(self):
        """

        Returns:

        """
        return self.ingestion.get_missing_files()

    def get_bash_command(self, date_missing_files):
        return self.ingestion.compose_bash_command(date_missing_files)

    def run_ingestion(self, bash_command, context):
        return self.ingestion.run_ingestion(bash_command, context)

    def submit_denorm_job(self, location_group, context):
        logging.info(location_group)
        logging.info(min(location_group) + "to" + max(location_group))
        batch_id = "-" + Variable.get(key="run_date").replace("_", "-") + "-" + datetime.now().strftime("%f")
        self.denorm.submit_dataproc_job(location_group, batch_id, context)
        return batch_id

    def submit_validator_job(self, ingested_files, missing_files, context):
        batch_id = "-" + Variable.get(key="run_date").replace("_", "-") + "-" + datetime.now().strftime("%f")
        self.validate.submit_dataproc_job(batch_id, ingested_files, missing_files, context)
        return batch_id

    def submit_thershold_job(self, missing_files, context):
        batch_id = "-" + Variable.get(key="run_date").replace("_", "-") + "-" + datetime.now().strftime("%f")
        self.threshold.submit_dataproc_job(batch_id, missing_files, context)
        return batch_id

    def submit_preprocess_job(self, valid_files, context):
        batch_id = "-" + Variable.get(key="run_date").replace("_", "-") + "-" + datetime.now().strftime("%f")
        self.preprocess.submit_dataproc_job(valid_files, batch_id, context)
        return batch_id

    def submit_e2e_validator_job(self, location_group, context):
        batch_id = "-" + Variable.get(key="run_date").replace("_", "-") + "-" + datetime.now().strftime("%f")
        self.e2e.submit_dataproc_job(location_group, batch_id, context)
        return batch_id

    def create_validator_cluster(self, context):
        self.validate.submit_manual_dataproc(context)

    def run_validator_job(self, context):
        self.validate.run_job(context)

    def delete_cluster(self, context):
        self.validate.delete_cluster(context)

    def get_location_groups(self):
        return self.preprocess.get_location_groups()

    def get_valid_files_after_ingestion(self):
        return self.ingestion.get_valid_files_after_ingestion()

    def delete_batch(self, component_name, batch_id, context):
        if component_name == constant.DENORM:
            return self.denorm.delete_dataproc_job(constant.DENORM, batch_id, context)
        elif component_name == constant.PREPROCESS:
            return self.preprocess.delete_dataproc_job(constant.PREPROCESS, batch_id, context)
        elif component_name == constant.E2E:
            return self.e2e.delete_dataproc_job(constant.E2E, batch_id, context)
        elif component_name == constant.VALIDATOR:
            return self.validate.delete_dataproc_job(constant.VALIDATOR, batch_id, context)
        elif component_name == constant.BUSINESS_FS:
            return self.bfs.delete_dataproc_job(constant.BUSINESS_FS, batch_id, context)
        # self.inference.trigger_inference(context)

    def submit_bfs_job(self, location_group, context):
        logging.info(location_group)
        logging.info(min(location_group) + "to" + max(location_group))
        batch_id = "-" + Variable.get(key="run_date").replace("_", "-") + "-" + datetime.now().strftime("%f")
        self.bfs.submit_dataproc_job(location_group, batch_id, context)
        return batch_id

    def denorm_merge(self, location_group, context):
        batch_id = "-" + Variable.get(key="run_date").replace("_", "-") + "-" + datetime.now().strftime("%f")
        self.merge.submit_dataproc_job(batch_id, location_group, constant.DENORM, context)

    def bfs_merge(self, location_group, context):
        batch_id = "-" + Variable.get(key="run_date").replace("_", "-") + "-" + datetime.now().strftime("%f")
        self.merge.submit_dataproc_job(batch_id, location_group, constant.BUSINESS_FS, context)
