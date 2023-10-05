from helpers import file_helpers, date_helpers, main_helpers, \
                    multiprocessing_helpers, list_helpers, string_helpers
from utils import file_utils, gcs_utils, log_utils


def assign_logger_to_files(logger: object):
    """
    The assign_logger_to_files function assigns the logger object to all of the helper and utility files.
    This is done so that each file can log messages, which makes it easier for debugging purposes.

    :param logger: object: Assign the logger to all of the files in this project
    """
    try:
        file_helpers.logger = logger
        logger.info("Logger assigned to helpers/file_helpers.py")
        list_helpers.logger = logger
        logger.info("Logger assigned to helpers/list_helpers.py")
        multiprocessing_helpers.logger = logger
        logger.info("Logger assigned to helpers/multiprocessing_helpers.py")
        main_helpers.logger = logger
        logger.info("Logger assigned to helpers/main_helpers.py")
        date_helpers.logger = logger
        logger.info("Logger assigned to helpers/date_helpers.py")
        string_helpers.logger = logger
        logger.info("Logger assigned to helpers/string_helpers.py")

        file_utils.logger = logger
        logger.info("Logger assigned to utils/file_utils.py")
        gcs_utils.logger = logger
        logger.info("Logger assigned to utils/gcs_utils.py")
        log_utils.logger = logger
        logger.info("Logger assigned to utils/log_utils.py")
    except Exception as e:
        logger.error(f"Logger not assigned to file : {e}")
        raise
