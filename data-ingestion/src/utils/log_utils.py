import os

import pandas as pd

import file_utils
import gcs_utils
import logger_utils

logger = None


def create_empty_csv(file_path: str, columns: list):
    """
    The create_empty_csv function creates an empty CSV file with the specified columns.

    :param file_path: str: Specify the path to the csv file
    :param columns: list: Specify the columns that will be present in the csv file
    """
    try:
        directory = os.path.dirname(file_path)

        if directory:
            os.makedirs(directory, exist_ok=True)

        df = pd.DataFrame(columns=columns)
        df.to_csv(file_path, index=False)
        logger.info("Empty CSV file created successfully.")
    except Exception as e:
        logger.error(f"Error occurred while creating the CSV file: {e}")
        raise


def add_row_to_csv(file_path: str, row_data: list):
    """
    The add_row_to_csv function takes in a file path and a list of data.
    It then reads the CSV file at the given path, creates a new row from the given data,
    and appends that row to the end of the CSV file. It returns nothing.

    :param file_path: str: Specify the path of the csv file
    :param row_data: list: Pass the data that will be added to the csv file
    """
    try:
        df = pd.read_csv(file_path)
        new_row = pd.DataFrame(row_data, columns=df.columns)
        updated_df = pd.concat([df, new_row], ignore_index=True)
        updated_df.to_csv(file_path, index=False)
        logger.info("Row added to CSV file successfully.")
    except Exception as e:
        logger.error(f"Error occurred while adding a row to the CSV file: {e}")
        raise


def is_dataframe_empty(df: object):
    """
    The is_dataframe_empty function checks if a DataFrame is empty.
        Args:
            df (object): The DataFrame to check for emptiness.

    :param df: object: Specify the type of object that is being passed into the function
    :return: True if the dataframe is empty, false otherwise
    """
    try:
        return df.empty
    except Exception as e:
        logger.error(f"Error occurred while checking if DataFrame is empty: {e}")
        raise


def read_csv_as_dataframe(file_path: str):
    """
    The read_csv_as_dataframe function reads a CSV file and returns the data as a Pandas DataFrame.

    :param file_path: str: Specify the path of the file to be read
    :return: A pandas dataframe
    """
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        logger.error(f"Error occurred while reading the CSV file: {e}")
        raise


def display_csv_contents(df: object):
    """
    The display_csv_contents function prints the contents of a DataFrame to the console.


    :param df: object: Pass the dataframe object to this function
    :return: The dataframe
    """
    try:
        logger.info(df)
    except Exception as e:
        logger.error("Error occurred while printing the DataFrame:", str(e))


def create_log_row(sub_proc_id, run_id, task_id, component, run_date, file_date, status, priority, error_code,
                   error_message, missed_files, processed_files, unpermitted_files, extra):
    """
    The create_log_row function creates a list of lists that will be used to create the log file.
        The function takes in the following parameters:
            sub_proc_id - A string representing the sub-process ID (e.g., '01', '02')
            run_id - A string representing the run ID (e.g., '201907021500')
            task_id - An integer representing a unique task identifier

    :param sub_proc_id: Identify the sub-process that is being run
    :param run_id: Create a unique id for each run of the process
    :param task_id: Identify the task in the log table
    :param component: Identify the component that is being logged
    :param run_date: Get the current date and time
    :param file_date: Determine the date of the files that are being processed
    :param status: Determine if the process was successful or not
    :param priority: Determine the order in which tasks are executed
    :param error_code: Identify the type of error that occurred
    :param error_message: Store the error message if any
    :param missed_files: Store the number of files that were missed in a run
    :param processed_files: Store the number of files processed in a run
    :param unpermitted_files: Store the files that are not permitted to be processed
    :param extra: Add extra information to the log row
    :return: A list of lists
    """
    try:
        return [[sub_proc_id, run_id, task_id, component, run_date, file_date, status, priority, error_code,
                 error_message, missed_files, processed_files, unpermitted_files, extra]]
    except Exception as e:
        logger.error(f"Error while creating a log row : {e}")
        raise


def audit_diagnostics_log(log_bucket_url,
                          columns,
                          rows_list,
                          component_name):
    """
    The audit_diagnostics_log function is used to log the results of a diagnostic audit.

    :param log_bucket_url: Specify the location of the log file
                            ex: 'gcs://bucketname/bucketpath/log/log.csv'
    :param columns: list: Define the columns of the csv file
                            ex: ['sub_proc_id', 'task_id', .....]
    :param rows_list: list(list()): Pass a list of lists to the function
                            ex: [ [1,2,'data',...], [1,3,'data',.....] ]
    :param component_name: Provide component name for logging ex: 'Data Ingestion'
    """
    try:
        log_bucket_name = gcs_utils.extract_bucket_name(log_bucket_url)
        log_bucket_path = gcs_utils.extract_bucket_path(log_bucket_url)
        log_file_name = gcs_utils.extract_bucket_file(log_bucket_url)
        temp_directory = file_utils.create_temp_directory()
        log_local_source_path = f"{temp_directory}/{log_file_name}"
        file_status = gcs_utils.check_file_exists_in_path(log_bucket_name, log_bucket_path, log_file_name)
        if file_status:
            gcs_utils.download_file_from_path(log_bucket_name, log_bucket_path, log_file_name, temp_directory)
        else:
            create_empty_csv(log_local_source_path, columns)
        add_row_to_csv(log_local_source_path, rows_list)
        destination_blob_name = f"{log_bucket_path}/{log_file_name}"
        gcs_utils.upload_file_to_path(
            log_bucket_name,
            log_local_source_path,
            destination_blob_name)
    except Exception as e:
        logger_ = logger_utils.setup_logger("DEBUG", component_name)
        logger_.error(f"Error while retrieving the log : {e}")
        raise
