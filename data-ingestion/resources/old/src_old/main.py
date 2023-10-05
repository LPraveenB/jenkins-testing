import sys_
from resources import config
import traceback
from helpers import main_helpers, logger_helpers, date_helpers
from utils import logger_utils, gcs_utils, file_utils, log_utils

if __name__ == "__main__":
    try:
        command_line_arguments_dict = main_helpers.get_command_line_arguments()
        sub_proc_id = command_line_arguments_dict["sub_proc_id"]
        run_id = command_line_arguments_dict["run_id"]
        task_id = command_line_arguments_dict["task_id"]
        input_path = command_line_arguments_dict["input_path"]
        error_path = command_line_arguments_dict["error_path"]
        output_path = command_line_arguments_dict["output_path"]
        log_path = command_line_arguments_dict["log_path"]
        date_missing_files = command_line_arguments_dict["date_missing_files"]

        log_file_local_dir = main_helpers.create_temp_directory()
        log_component_name = config.log_component_name
        log_component_column = config.log_component_column
        log_level = config.log_level
        log_bucket_name = gcs_utils.extract_bucket_name(log_path)
        log_folder_name = gcs_utils.extract_bucket_path(log_path)
        log_file_name = gcs_utils.extract_bucket_file(log_path)
        log_file_columns = config.log_file_columns
        log_error_names = config.log_error_names
        log_error_column = config.log_error_column
        permitted_extensions = config.permitted_extensions
        log_file_local_path = f"{log_file_local_dir}/{log_file_name}"
        readme_file_content = config.readme_file_content
        readme_file_name = config.readme_file_name
        file_day = config.file_day
        log_file_date_format = config.log_file_date_format
        output_folder_date_format = config.output_folder_date_format
        required_files = config.required_files
        special_files = config.special_files
        acceptable_date_formats_in_file_names = config.acceptable_date_formats_in_file_names
        error_messages_dict = config.error_messages_dict
        log_content_display = config.log_content_display
        log_run_date_column = config.log_run_date_column
        log_file_date_column = config.log_file_date_column
        log_missed_files_column = config.log_missed_files_column
        holidays_list = config.holidays_list

        logger = logger_utils.setup_logger(log_level, log_component_name)
        logger_helpers.assign_logger_to_files(logger)

        main_helpers.get_log_ready(log_bucket_name, log_folder_name, log_file_local_path,
                                   log_file_columns)

        filtered_log_df = main_helpers.get_last_run_rows(log_file_local_path, log_run_date_column,
                                                         log_file_date_format, log_component_column,
                                                         log_component_name, log_error_names, log_error_column)
        date_range_list = main_helpers.get_unique_dates(filtered_log_df, log_file_date_column)
        date_range_list = main_helpers.sort_date_strings(date_range_list,
                                                         log_file_date_format)
        input_dates_list = list(date_missing_files.keys())
        input_dates_list = main_helpers.sort_date_strings(input_dates_list,
                                                          log_file_date_format)
        final_dates_with_holidays = main_helpers.fill_dates(date_range_list, input_dates_list, log_file_date_format)
        final_dates_with_holidays = main_helpers.combine_and_remove_duplicates(final_dates_with_holidays,
                                                                               [date_helpers.get_date(
                                                                                   file_day,
                                                                                   log_file_date_format)])
        sorted_final_dates_with_holidays = main_helpers.sort_date_strings(final_dates_with_holidays,
                                                                          log_file_date_format)
        final_dates = main_helpers.exclude_holidays(sorted_final_dates_with_holidays, holidays_list)
        main_helpers.process_files(final_dates,
                                   log_file_date_format,
                                   acceptable_date_formats_in_file_names,
                                   input_path,
                                   error_path,
                                   output_path,
                                   required_files,
                                   special_files,
                                   sub_proc_id,
                                   run_id,
                                   task_id,
                                   error_messages_dict,
                                   log_file_local_path,
                                   log_component_name,
                                   readme_file_name,
                                   readme_file_content,
                                   permitted_extensions
                                   )
        gcs_utils.upload_file_to_path(log_bucket_name, log_file_local_path, f"{log_folder_name}/{log_file_name}")
        main_helpers.log_display(log_content_display, log_file_local_path)
    except Exception as e:
        logger.critical(f"Unexpected error occurs in main : {e}")
        traceback.print_exc()
