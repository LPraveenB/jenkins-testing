log_file_columns = [
    "sub_proc_id",
    "run_id",
    "task_id",
    "component",
    "run_date",
    "file_date",
    "status",
    "priority",
    "error_code",
    "error_message",
    "missed_files",
    "processed_files",
    "unpermitted_files",
    "extra"
]

log_run_date_column = "run_date"
log_file_date_column = "file_date"
log_missed_files_column = "missed_files"

log_component_column = "component"
log_component_name = "Data Ingestion"

log_error_column = "priority"
log_error_names = ["critical"]

log_content_display = True
log_level = "DEBUG"

required_files = [
    "FD_STORE_INV"
]
special_files = [
    "FD_SALES",
    "FD_CARRIER_DETAILS",
    "FD_SKU_MASTER",
    "FD_RETURNS",
    "FD_PHYSICAL_INVENTORY_COUNT_SCHEDULE",
    "FD_INV_ADJ",
    "FD_PHYSICAL_INV_ADJUSTMENTS",
    "FD_STORE_TRANSFERS_IN",
    "FD_DSD_INVOICE"
]
permitted_extensions = ['csv', 'gz']

file_day = 0
log_file_date_format = "%Y%m%d"
output_folder_date_format = "%Y%m%d",
acceptable_date_formats_in_file_names = ["%d%m%Y", "%Y%m%d"]
holidays_list = ['20230704', '20230702']

error_messages_dict = {"DATA_ING_ERR_001": {"message": "All Files missed for the day",
                                            "status": "failed",
                                            "priority": "critical"},
                       "DATA_ING_ERR_002": {"message": "Mandatory files missed for the day",
                                            "status": "failed",
                                            "priority": "critical"}
                       }

readme_file_name = "readme.txt"
readme_file_content = """
                        1. **Avoid Special Characters**: Avoid using special characters such as 
                        `!@#$%^&*(){}[]<>?~` in the file names. Stick to alphanumeric characters, 
                        hyphens, and underscores.
                        2. **Limit File Name Length**: Keep the file name reasonably short and descriptive. 
                        Long file names can become difficult to manage and cause compatibility issues across 
                        different platforms.
                        3. **Use Meaningful Names**: Choose a file name that accurately reflects the content of the 
                        text file.
                      """
