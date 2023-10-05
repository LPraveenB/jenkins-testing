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
        list_helpers.logger = logger
        multiprocessing_helpers.logger = logger
        main_helpers.logger = logger
        date_helpers.logger = logger
        string_helpers.logger = logger

        file_utils.logger = logger
        gcs_utils.logger = logger
        log_utils.logger = logger
    except Exception as e:
        logger.error(f"Logger not assigned to file : {e}")
        raise
