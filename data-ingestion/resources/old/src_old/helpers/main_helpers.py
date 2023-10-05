import argparse
import pandas as pd
import ast
import uuid
from multiprocessing import Manager
from datetime import datetime, timedelta
import tempfile
import helpers
import utils
import os

logger = None


def get_command_line_arguments():
    """
    The get_command_line_arguments function parses command-line arguments and assigns them to variables.

    :return: A tuple of 4 variables
    """
    try:
        # Parse command-line arguments
        parser = argparse.ArgumentParser(
            description='Data Ingestion')
        parser.add_argument('--date_missing_files',
                            dest='date_missing_files',
                            type=str,
                            required=True,
                            help='Date key and Missing File Names Value Dict')
        parser.add_argument('--input_path', dest='input_path', type=str, required=True, help='GCS Input Bucket Path')
        parser.add_argument('--error_path', dest='error_path', type=str, required=True, help='GCS error Bucket Path')
        parser.add_argument('--output_path', dest='output_path', type=str, required=True, help='GCS Output Bucket Path')
        parser.add_argument('--log_path', dest='log_path', type=str, required=True, help='GCS Log Bucket Path')
        parser.add_argument('--sub_proc_id', dest='sub_proc_id', type=str, required=True, help='Sub Process ID')
        parser.add_argument('--run_id', dest='run_id', type=str, required=True, help='Run ID')
        parser.add_argument('--task_id', dest='task_id', type=str, required=True, help='Task ID')
        args = parser.parse_args()

        # Assign command-line arguments to variables
        sub_proc_id = args.sub_proc_id
        run_id = args.run_id
        task_id = args.task_id
        input_path = args.input_path
        error_path = args.error_path
        output_path = args.output_path
        log_path = args.log_path
        date_missing_files = ast.literal_eval(args.date_missing_files)

        return {"sub_proc_id": sub_proc_id,
                "run_id": run_id,
                "task_id": task_id,
                "input_path": input_path,
                "error_path": error_path,
                "output_path": output_path,
                "log_path": log_path,
                "date_missing_files": date_missing_files}
    except Exception as e:
        logger.error(f"Error while parsing command-line arguments : {e}")
        raise


def get_log_ready(bucket_name: str, bucket_path: str, local_path: str, columns: list):
    """
    The get_log_ready function is used to retrieve the log file from GCS and store it locally.
        If the log file does not exist, then an empty csv will be created with the specified columns.

    :param bucket_name: str: Specify the name of the bucket where we want to store our log file
    :param bucket_path: str: Specify the path in the bucket where you want to store your file
    :param local_path: str: Store the log file locally ex: tmp/wetwu/log.csv
    :param columns: list: Create the header of the log file
    :return: The file_status
    """
    try:
        local_path, file_name = os.path.split(local_path)
        file_status = utils.gcs_utils.check_file_exists_in_path(bucket_name, bucket_path, file_name)
        if file_status:
            utils.gcs_utils.download_file_from_path(bucket_name, bucket_path, file_name, local_path)
        else:
            utils.log_utils.create_empty_csv(f"{local_path}/{file_name}", columns)
    except Exception as e:
        logger.error(f"Error while retrieving the log : {e}")
        raise


def get_last_run_rows(file_path, date_column, input_format, log_component_column, log_component_name,
                      log_error_names, log_error_column):
    """
    The get_last_run_rows function retrieves the rows for the last date in a log file.
    The function takes as input:
        - The path to the log file (file_path)
        - The name of the column containing dates (date_column)
        - The format of dates in that column (input_format)
        - The name of the component whose logs are being retrieved from this file (log_component_name)
            and its corresponding column name in this DataFrame(log_component_column). This is used to filter out rows
            that do not belong to this component.

    :param file_path: Specify the path of the log file
    :param date_column: Specify the column in the dataframe that contains date values
    :param input_format: Specify the format of the date column in the log file
    :param log_component_column: Filter the log file by a specific component
    :param log_component_name: Filter the rows in the log file for a specific component
    :param log_error_names: Filter the rows for a specific error type
    :param log_error_column: Filter the rows of the dataframe by log error name
    :return: A dataframe containing the rows for the last date
    """
    try:
        # Step 1: Load the file into a DataFrame
        df = pd.read_csv(file_path)
        df = df.astype(str)
        df = df[(df[log_component_column] == log_component_name) & (df[log_error_column].isin(log_error_names))]
        # Step 2: Convert the date column to DateTime data type
        df[date_column] = pd.to_datetime(df[date_column], format=input_format)

        # Step 3: Sort the DataFrame by the date column in descending order
        df.sort_values(date_column, ascending=False, inplace=True)

        # Step 4: Retrieve the rows for the last date
        last_date = df[date_column].max()
        last_date_rows = df[df[date_column] == last_date]
        last_date_rows[date_column] = df[date_column].dt.strftime(input_format)
        # Return the DataFrame containing the rows for the last date
        return last_date_rows

    except Exception as e:
        logger.error("An error occurred while retrieving the last run rows from log file : ", str(e))
        raise


def flatten_list(input_list):
    """
    The flatten_list function takes a list of strings and returns a single string with all the values from the input
    list concatenated together. The function is used to flatten lists that are stored as strings in the database.


    :param input_list: Pass the list that needs to be flattened
    :return: A string representation of the flattened list
    """
    try:
        result = []
        for item in input_list:
            if pd.notnull(item) and item:  # Skip if item is null/NaN or an empty string
                values = ast.literal_eval(item)
                result.extend(values)
        return ','.join(list(set(result)))
    except Exception as e:
        logger.error(f"Error occurred while flattening list : {e}")
        raise


def get_missing_files_for_date_from_log(df, filter_column_name, target_column_name, target_value):
    """
    The get_missing_files_for_date_from_log function takes in a dataframe, filter_column_name, target_column_name and
    target value as input. It filters the dataframe based on the filter column name and target value. Then it returns
    the list of values from the filtered dataframe for the given target column name.

    :param df: Filter the dataframe
    :param filter_column_name: Filter the dataframe based on a column value
    :param target_column_name: Filter the column from the dataframe
    :param target_value: Filter the dataframe based on the target_column_name
    :return: A list of files that are missing for a particular date
    """
    try:
        return flatten_list(df.loc[df[filter_column_name] == target_value, target_column_name].tolist())
    except Exception as e:
        logger.error(f"Error occurred while filtering the column from log : {e}")
        raise


def get_missing_files_dict_from_log(date_missing_files_dict, filtered_log_df, filter_column_name, target_column_name):
    """
    The get_missing_files_dict_from_log function takes in a dictionary of dates and missing files,
    a filtered log dataframe, the name of the column to filter on (filter_column_name), and the name
    of the column containing file names (target_column_name). It returns a dictionary with dates as keys
    and lists of missing files for each date as values.

    :param date_missing_files_dict: Get the dates for which we need to find missing files
    :param filtered_log_df: Filter the log dataframe by a specific date
    :param filter_column_name: Filter the log_df by date
    :param target_column_name: Specify the column name in the log file that contains the list of files
    :return: A dictionary with the following structure
    """
    try:
        log_dates_missing_files = {}
        for key in date_missing_files_dict.keys():
            log_dates_missing_files[key] = get_missing_files_for_date_from_log(filtered_log_df, filter_column_name,
                                                                               target_column_name, key)
        return log_dates_missing_files
    except Exception as e:
        logger.error(f"Error occurred while retrieving log_dates_missing_files : {e}")
        raise


def check_dict_equality(input_dict,
                        log_dict):
    """
    The check_dict_equality function takes two dictionaries as input and checks if they are equal.
        If the dictionaries are not equal, it raises an exception with a message that includes both
        of the input dictionaries.

    :param input_dict: Pass the input dictionary to be compared with log_dict
    :param log_dict: Compare the input_dict parameter
    :return: True if the two dictionaries are equal, and false otherwise
    """
    try:
        if input_dict == log_dict:
            return True
        else:
            raise Exception(f"input dict : {input_dict}  | log dict : {log_dict} Not Matching")
    except Exception as e:
        logger.error(f"Error while checking the dict equality : {e}")
        raise


def get_unique_dates(log_filtered_df, date_column):
    """
    The get_unique_dates function takes in a dataframe, the name of the column containing dates,
    the day of the log file being processed (e.g., '2019-01-01'), and a date format string for parsing
    dates from that column. It returns a list of unique dates found in that dataframe.

    :param log_filtered_df: Filter the dataframe for a particular date
    :param date_column: Specify the column in the dataframe that contains the date
    :return: A list of dates in the format specified by log_file_date_format
    """
    try:
        if not utils.log_utils.is_dataframe_empty(log_filtered_df):
            unique_dates = log_filtered_df[date_column].unique().tolist()
            return unique_dates
        else:
            return []
    except Exception as e:
        logger.error(f"Error while retrieving the unique dates : {e}")
        raise


def fill_dates(first_list, second_list, log_file_date_format):
    """
    The fill_dates function takes in three parameters:
        1. first_list - a list of dates that are already present in the database
        2. second_list - a list of dates that are not yet present in the database, but need to be added
        3. log_file_date_format - this is used to generate date ranges between two given dates

    :param first_list: Pass the list of dates from the previous log file
    :param second_list: Fill the dates between the first_list and second_list
    :param log_file_date_format: Specify the format of the date in log file
    :return: A list of dates that are in the range of the first and second lists
    """
    try:
        if first_list and second_list:
            combined_list = combine_and_remove_duplicates(first_list, second_list)
            max_date, min_date = get_max_min_dates(combined_list, log_file_date_format)
            min_date = datetime.strptime(min_date, log_file_date_format)
            max_date = datetime.strptime(max_date, log_file_date_format)
            if max_date > min_date:
                return generate_date_range(min_date, max_date, log_file_date_format)
            else:
                return combine_and_remove_duplicates(first_list, second_list)
        else:
            return combine_and_remove_duplicates(first_list, second_list)
    except Exception as e:
        logger.error(f"Error while filling dates : {e}")
        raise


def get_max_min_dates(date_list, date_format):
    """
    The get_max_min_dates function takes in a list of dates and returns the maximum and minimum date.

    :param date_list: Pass the list of dates to be converted
    :param date_format: Convert the dates in date_list to datetime objects
    :return: The maximum and minimum dates in the date_list
    """
    try:
        # Convert the dates to datetime objects
        date_objects = [datetime.strptime(date, date_format) for date in date_list]

        # Get the maximum and minimum dates
        max_date = max(date_objects).strftime(date_format)
        min_date = min(date_objects).strftime(date_format)

        return max_date, min_date
    except Exception as e:
        logger.error(f"Error while getting max min dates : {e}")
        raise


def generate_date_range(start_date, end_date, date_format):
    """
    The generate_date_range function takes in a start date, end date and log file format as input.
    It then generates a list of dates between the start and end dates based on the log file format.
    The function returns an empty list if the end_date is less than start_date.

    :param start_date: Specify the start date of the range
    :param end_date: Specify the end date of the range
    :param date_format: Format the date in a specific way
    :return: A list of dates in the format specified by log_file_date_format
    """
    try:
        start_datetime = start_date
        end_datetime = end_date
        date_range = []
        current_datetime = start_datetime

        while current_datetime <= end_datetime:
            date_range.append(current_datetime.strftime(date_format))
            current_datetime += timedelta(days=1)

        return date_range
    except Exception as e:
        logger.error(f"Error while generating the date range : {e}")
        raise


def exclude_holidays(date_range_list, holidays_list):
    """
    The exclude_holidays function takes in a list of dates and a list of holidays,
    and returns the original date range with all holidays removed.


    :param date_range_list: Pass the list of dates between start and end date
    :param holidays_list: Exclude the holidays from the date range
    :return: A list of dates excluding the holidays
    """
    try:
        return [date for date in date_range_list if date not in holidays_list]
    except Exception as e:
        logger.error(f"Error while excluding holidays from the date range : {e}")
        raise


def create_temp_directory():
    """
    The create_temp_directory function creates a temporary directory and returns the path to it.

    :return: The path of the newly created temporary directory
    """
    try:
        temp_dir = tempfile.mkdtemp()
        return temp_dir
    except Exception as e:
        logger.error(f"Error while creating temp directory : {e}")
        raise


def sort_date_strings(date_strings, date_format):
    """
    The sort_date_strings function takes a list of date strings and sorts them in chronological order.

    :param date_strings: Pass the list of date strings to be sorted
    :param date_format: Specify the format of the date strings
    :return: A list of sorted date strings
    """
    try:
        # Convert date strings to datetime objects
        dates = [datetime.strptime(date_str, date_format) for date_str in date_strings]

        # Sort the datetime objects
        sorted_dates = sorted(dates)

        # Convert the sorted datetime objects back to date strings
        sorted_date_strings = [date.strftime(date_format) for date in sorted_dates]

        return sorted_date_strings
    except Exception as e:
        logger.error(f"Error: Invalid date format or date string : {e}")
        raise


def combine_and_remove_duplicates(*lists):
    """
    The combine_and_remove_duplicates function takes any number of lists as input and returns a single list with all the
    elements from the input lists. If there are any duplicate values, they will be removed from the final list.

    :param lists: Pass any number of lists to be combined and have duplicates removed
    :return: A list of unique values from the input lists combined
    """
    try:
        combined = list(set().union(*lists))
        return combined
    except Exception as e:
        logger.error(f"Error while combining lists: {e}")
        raise


def process_each_file(date_val,
                      processed_files,
                      file,
                      reference_list,
                      source_bucket,
                      source_bucket_path,
                      output_bucket,
                      output_bucket_path
                      ):
    """
    The process_each_file function is used to process each file in the source bucket.
        It takes the following parameters:
            date_val : The date value for which files are being processed.
            missed_files : A list of files that were not found in the source bucket.  This list will be updated with
            any missing files during processing.
            processed_files : A list of all successfully processed files from the source bucket, this will be updated
            as each file is successfully processed and uploaded to GCS.  This can also be used as a reference for what
            was actually moved over if there are issues with downstream processes or data quality checks

    :param date_val: Create a directory in the output bucket
    :param processed_files: Keep track of the files that have been processed
    :param file: Get the file name from the list of files in a bucket
    :param reference_list: Filter the files to be processed
    :param source_bucket: Specify the source bucket name
    :param source_bucket_path: Specify the path in the source bucket where files are stored
    :param output_bucket: Specify the destination bucket
    :param output_bucket_path: Create a directory structure in the output bucket
    :return: A list of processed files
    """
    try:
        for reference_file in reference_list:
            if reference_file in file:
                processed_files.append(reference_file)
                temp_directory = create_temp_directory()
                # unique_id = str(uuid.uuid4().hex[:6])
                # # Split the file name into base name and extension
                # base_name, extension = file.split('.', 1)
                # # Add UUID to the base name
                # new_file_name = f"{base_name}-{unique_id}.{extension}"
                destination_blob_name = f"{output_bucket_path}/{date_val}/{reference_file}/{file}"
                local_file_path = f"{temp_directory}/{file}"
                utils.gcs_utils.download_file_from_path(source_bucket, source_bucket_path, file, temp_directory)
                utils.gcs_utils.upload_file_to_path(output_bucket, local_file_path, destination_blob_name)
                utils.file_utils.remove_directory(temp_directory)
    except Exception as e:
        logger.error(f"Error while processing each file : {e}")
        raise


def process_each_day(date_val,
                     missed_files,
                     processed_files,
                     files_to_process,
                     reference_list,
                     source_bucket,
                     source_bucket_path,
                     error_bucket,
                     error_bucket_path,
                     output_bucket,
                     output_bucket_path
                     ):
    """
    The process_each_day function is used to process each day's files.
        It takes the following arguments:
            date_val : The date for which the files are being processed.
            missed_files : A list of all reference files that were not found in the source bucket.  This list will be
            updated as reference files are found in source bucket, and if no more missed references remain, then we can
            proceed with processing these days' data; otherwise, we must move on to another day's data (or exit).
            processed_files : A list of all file names that have been successfully processed so far during this run.

    :param date_val: Get the date for which we are processing the files
    :param missed_files: Store the files that are not present in the source bucket
    :param processed_files: Store the files that have been processed
    :param files_to_process: Pass the list of files to be processed for each day
    :param reference_list: Get the list of files to be processed for each day
    :param source_bucket: Get the bucket name of the source folder
    :param source_bucket_path: Get the list of files in the source bucket
    :param error_bucket: Store the files that have errors
    :param error_bucket_path: Store the files in error bucket
    :param output_bucket: Store the output files
    :param output_bucket_path: Store the output files in a specific path
    """
    try:
        for reference_file in reference_list:
            for file_to_process in files_to_process:
                if reference_file in file_to_process:
                    if reference_file in missed_files:
                        missed_files.remove(reference_file)
        if not missed_files:
            output_bucket, output_bucket_path = output_bucket, output_bucket_path
        else:
            output_bucket, output_bucket_path = error_bucket, error_bucket_path
        final_list = [((date_val,
                        processed_files,
                        file_to_process,
                        reference_list,
                        source_bucket,
                        source_bucket_path,
                        output_bucket,
                        output_bucket_path
                        )) for file_to_process in files_to_process]
        pool = helpers.multiprocessing_helpers.get_multiprocessing_pool()
        pool.starmap(process_each_file, final_list)
        helpers.multiprocessing_helpers.close_multiprocessing_pool(pool)
        helpers.multiprocessing_helpers.join_multiprocessing_pool(pool)
    except Exception as e:
        logger.error(f"Error while processing for each day : {e}")
        raise


def separate_files_by_extension(file_list, target_extensions):
    """
    The separate_files_by_extension function takes in a list of files and a list of target extensions.
    It returns two lists, one containing the files with the target extensions and another containing all other files.

    :param file_list: Pass the list of files to be seperated
    :param target_extensions: Pass the list of extensions that we want to separate from the file_list
    :return: A tuple of two lists
    """
    try:
        target_files = []
        other_files = []

        for file in file_list:
            extension = file.split('.')[-1].lower()
            if extension in target_extensions:
                target_files.append(file)
            else:
                other_files.append(file)

        return target_files, other_files
    except Exception as e:
        logger.error(f"Error while seperating the files by extension : {e}")
        raise


def process_unpermitted_files(date_val, files_list, reference_list, error_bucket, error_bucket_path,
                              sub_proc_id, run_id, task_id, log_component_name, log_date_format, source_bucket,
                              source_bucket_path, log_file_path):
    """
    The process_unpermitted_files function is used to move the unpermitted files from source bucket to error bucket.
        Args:
            date_val (str): The date value for which the process has been run.
            files_list (list): List of all the file names present in source bucket path.
            reference_list (list): List of all permitted file names that are expected in source bucket path.
            error_bucket(str) : Name of GCS Bucket where unpermitted files will be moved to .
            error_bucket_path(str) : Path inside GCS Bucket

    :param date_val: Create the error bucket path
    :param files_list: Store the list of files that are present in the source bucket path
    :param reference_list: Check if the file in files_list is permitted or not
    :param error_bucket: Store the files that are not permitted to be processed
    :param error_bucket_path: Specify the path in the error bucket where we want to upload
    :param sub_proc_id: Create a unique identifier for the log file
    :param run_id: Create a unique log file for each run of the process
    :param task_id: Identify the task in the log file
    :param log_component_name: Specify the name of the component in which this function is being used
    :param log_date_format: Format the date to be used in the log file
    :param source_bucket: Specify the bucket from which files are to be downloaded
    :param source_bucket_path: Specify the path of the source bucket
    :param log_file_path: Create a log file in the bucket
    :return: A list of unpermitted files
    """
    try:
        if files_list:
            unpermitted_files = []
            for file_ in files_list:
                for reference_file in reference_list:
                    if reference_file in file_:
                        unpermitted_files.append(file_)
                        temp_directory = create_temp_directory()
                        destination_blob_name = f"{error_bucket_path}/{date_val}/{reference_file}/{file_}"
                        local_file_path = f"{temp_directory}/{file_}"
                        utils.gcs_utils.download_file_from_path(source_bucket, source_bucket_path, file_,
                                                                temp_directory)
                        utils.gcs_utils.upload_file_to_path(error_bucket, local_file_path, destination_blob_name)
                        utils.file_utils.remove_directory(temp_directory)
            if unpermitted_files:
                row_data = utils.log_utils.create_log_row(sub_proc_id,
                                                          run_id,
                                                          task_id,
                                                          log_component_name,
                                                          datetime.today().strftime(log_date_format),
                                                          date_val,
                                                          "failed",
                                                          "moderate",
                                                          "",
                                                          "",
                                                          "",
                                                          "",
                                                          unpermitted_files,
                                                          ""
                                                          )
                utils.log_utils.add_row_to_csv(log_file_path, row_data)
    except Exception as e:
        logger.error(f"Error while processing unpermitted files : {e}")
        raise


def remove_file(file_list, file_to_remove):
    """
    The remove_file function takes in a list of files and removes the file_to_remove from that list.
        Args:
            file_list (list): A list of files to be removed from.
            file_to_remove (str): The name of the file to remove from the list.

    :param file_list: Pass the list of files to be removed
    :param file_to_remove: Pass the file name to be removed from the list
    :return: The list of files after removing the file_to_remove from it
    """
    try:
        file_list = [file for file in file_list if file != file_to_remove]
        return file_list
    except Exception as e:
        logger.error(f"Error while removing file : {e}")
        raise


def process_files(dates_list,
                  log_file_format,
                  date_formats,
                  input_path,
                  error_path,
                  output_path,
                  required_files,
                  special_files,
                  sub_proc_id,
                  run_id,
                  task_id,
                  error_messages_dict,
                  log_file_path,
                  log_component_name,
                  readme_file_name,
                  readme_file_content,
                  permitted_extensions
                  ):
    """
    The process_files function is the main function of this module.
    It takes in a list of dates, and processes all files for each date.
    The process_files function calls the process_each_day function to handle processing for each day.
    The process_files function also handles logging and error handling.

    :param dates_list: Pass the list of dates to be processed
    :param log_file_format: Specify the format of the log file
    :param date_formats: Convert the date format to a list of formats
    :param input_path: Specify the path to the input files
    :param error_path: Specify the path where all files that are not required should be moved to
    :param output_path: Specify the path where the processed files are to be moved
    :param required_files: Specify the list of files that are required to be present in the input path
    :param special_files: Specify the files that are not required to be present in every date folder
    :param sub_proc_id: Identify the sub-process in which this function is running
    :param run_id: Uniquely identify the run
    :param task_id: Identify the task in the log file
    :param error_messages_dict: Store the error messages for each file
    :param log_file_path: Specify the path where the log file will be stored
    :param log_component_name: Identify the component in the log file
    :param readme_file_name: Create a readme file in the source path
    :param readme_file_content: Create a readme file in the source path
    :param permitted_extensions: Check if the file extension is permitted or not
    :return: A list of dates for which the files were processed
    """
    try:
        source_bucket, source_bucket_path = utils.gcs_utils.extract_bucket_name(
            input_path), utils.gcs_utils.extract_bucket_path(input_path)
        error_bucket, error_bucket_path = utils.gcs_utils.extract_bucket_name(
            error_path), utils.gcs_utils.extract_bucket_path(error_path)
        output_bucket, output_bucket_path = utils.gcs_utils.extract_bucket_name(
            output_path), utils.gcs_utils.extract_bucket_path(output_path)
        all_files = utils.gcs_utils.list_files_in_path(source_bucket, source_bucket_path)
        all_files = remove_file(all_files, readme_file_name)
        reference_list = combine_and_remove_duplicates(required_files, special_files)
        for date_val in dates_list:
            missed_files = Manager().list(required_files)
            processed_files = Manager().list()
            files_to_process = []
            date_formats_list = helpers.date_helpers.convert_date_formats(date_val, log_file_format, date_formats)
            for date_format in date_formats_list:
                for each_file in all_files:
                    if date_format in each_file:
                        files_to_process.append(each_file)
            files_to_process, other_files = separate_files_by_extension(files_to_process, permitted_extensions)
            process_each_day(date_val,
                             missed_files,
                             processed_files,
                             files_to_process,
                             reference_list,
                             source_bucket,
                             source_bucket_path,
                             error_bucket,
                             error_bucket_path,
                             output_bucket,
                             output_bucket_path
                             )
            add_logs(date_val,
                     missed_files,
                     processed_files,
                     required_files,
                     sub_proc_id,
                     run_id,
                     task_id,
                     log_file_format,
                     error_messages_dict,
                     log_file_path,
                     log_component_name)
            process_unpermitted_files(date_val, other_files, reference_list, error_bucket, error_bucket_path,
                                      sub_proc_id, run_id, task_id, log_component_name, log_file_format, source_bucket,
                                      source_bucket_path, log_file_path)
        utils.gcs_utils.delete_path(source_bucket, source_bucket_path)
        handle_empty_source_path(readme_file_name,
                                 readme_file_content,
                                 source_bucket,
                                 source_bucket_path)
    except Exception as e:
        logger.error(f"Error while processing dates : {e}")
        raise


def handle_empty_source_path(readme_file_name,
                             readme_file_content,
                             source_bucket,
                             source_bucket_path):
    """
    The handle_empty_source_path function is used to handle the case where there is no source path.
        This function will create a temporary directory, save the readme file content in it and upload it to GCS.

    :param readme_file_name: Create a file with the given name in the temp directory
    :param readme_file_content: Save the readme file content to a temporary directory
    :param source_bucket: Upload the readme file to the source bucket
    :param source_bucket_path: Create the destination_blob_name
    """
    try:
        file_path = f"{create_temp_directory()}/{readme_file_name}"
        destination_blob_name = f"{source_bucket_path}/{readme_file_name}"
        utils.file_utils.save_to_file(file_path, readme_file_content)
        utils.gcs_utils.upload_file_to_path(source_bucket, file_path, destination_blob_name)
    except Exception as e:
        logger.error(f"Error while Handling empty source path : {e}")
        raise


def add_logs(date_val,
             missed_files,
             processed_files,
             required_files,
             sub_proc_id,
             run_id,
             task_id,
             log_date_format,
             error_messages_dict,
             log_file_path,
             log_component_name):
    """
    The add_logs function is used to add logs for the data ingestion process.
        It takes in the following parameters:
            date_val : The date value for which we are adding logs.
            missed_files : A list of files that were not found in the source directory.
            processed_files : A list of files that were successfully processed and moved to destination directory.
            required_files : A list of all required files as per configuration file, irrespective if they exist or not.

    :param date_val: Get the date for which the files are being processed
    :param missed_files: Store the list of files that were not present in the source directory
    :param processed_files: Store the list of files which were successfully processed
    :param required_files: Get the list of files that are required to be processed
    :param sub_proc_id: Identify the sub-process id
    :param run_id: Identify the run of the process
    :param task_id: Identify the task in the log file
    :param log_date_format: Format the date in a specific way
    :param error_messages_dict: Store the error messages for each error code
    :param log_file_path: Store the log file path
    :param log_component_name: Identify the component name in the log file
    """
    try:
        missed_files = list(missed_files)
        processed_files = list(processed_files)
        if missed_files == required_files:
            row_data = utils.log_utils.create_log_row(sub_proc_id,
                                                      run_id,
                                                      task_id,
                                                      log_component_name,
                                                      datetime.today().strftime(log_date_format),
                                                      date_val,
                                                      error_messages_dict["DATA_ING_ERR_001"]["status"],
                                                      error_messages_dict["DATA_ING_ERR_001"]["priority"],
                                                      "DATA_ING_ERR_001",
                                                      error_messages_dict["DATA_ING_ERR_001"]["message"],
                                                      missed_files,
                                                      "",
                                                      "",
                                                      ""
                                                      )
            utils.log_utils.add_row_to_csv(log_file_path, row_data)
        elif len(required_files) > len(missed_files) > 0:
            row_data = utils.log_utils.create_log_row(sub_proc_id,
                                                      run_id,
                                                      task_id,
                                                      log_component_name,
                                                      datetime.today().strftime(log_date_format),
                                                      date_val,
                                                      error_messages_dict["DATA_ING_ERR_002"]["status"],
                                                      error_messages_dict["DATA_ING_ERR_002"]["priority"],
                                                      "DATA_ING_ERR_002",
                                                      error_messages_dict["DATA_ING_ERR_002"]["message"],
                                                      missed_files,
                                                      "",
                                                      "",
                                                      ""
                                                      )
            utils.log_utils.add_row_to_csv(log_file_path, row_data)

        if len(processed_files) > 0:
            row_data = utils.log_utils.create_log_row(sub_proc_id,
                                                      run_id,
                                                      task_id,
                                                      log_component_name,
                                                      datetime.today().strftime(log_date_format),
                                                      date_val,
                                                      "success",
                                                      "",
                                                      "",
                                                      "",
                                                      "",
                                                      processed_files,
                                                      "",
                                                      ""
                                                      )
            utils.log_utils.add_row_to_csv(log_file_path, row_data)
    except Exception as e:
        logger.error(f"Error while adding rows to log : {e}")
        raise


def log_display(log_content_display, log_file_local_path):
    """
    The log_display function is used to display the contents of a log file.
        Args:
            log_content_display (bool): A boolean value indicating whether or not to display the contents of a log file.
            log_file_local_path (str): The local path where the desired log file is located.

    :param log_content_display: Display the log file contents
    :param log_file_local_path: Specify the path of the log file
    """
    try:
        if log_content_display:
            df = utils.log_utils.read_csv_as_dataframe(log_file_local_path)
            utils.log_utils.display_csv_contents(df)
    except Exception as e:
        logger.error(f"Error while displaying log : {e}")
        raise
