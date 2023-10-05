import os
import shutil
import zipfile
import uuid
import utils

logger = None


def extract_zip_recursive(zip_file_path: str, output_path: str):
    """
    The extract_zip_recursive function takes a zip file and extracts it to the output path.
    It then recursively processes all files in the extracted folder, moving them to their appropriate folders.


    :param zip_file_path: str: Specify the path to the zip file
    :param output_path: str: Specify the output path for the extracted files
    :return: A list of the extracted files
    """
    try:
        temp_folder = create_temp_folder(output_path)

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_folder)

            process_files_recursively(temp_folder, output_path)

    except Exception as e:
        logger.error(f"Error occurred during extraction: {e}")
        raise

    finally:
        utils.file_utils.remove_directory(temp_folder)


def create_temp_folder(output_path: str):
    """
    The create_temp_folder function creates a temporary folder in the output path.
        The function takes an output_path argument and returns a temp_folder variable.
        If an error occurs during the creation of the temporary folder, it will be logged and raised.

    :param output_path: str: Specify the path to the folder where we want to create a temporary folder
    :return: A string with the path to a temporary folder
    """
    try:
        temp_folder = os.path.join(output_path, str(uuid.uuid4())[:8])
        os.makedirs(temp_folder)
        return temp_folder

    except Exception as e:
        logger.error(f"Error occurred during temporary folder creation: {e}")
        raise


def process_files_recursively(temp_folder: str, output_path: str):
    """
    The process_files_recursively function takes in a temporary folder and an output path.
    It then iterates through the files in the temporary folder, checking if they are zip files.
    If they are, it extracts them recursively into the output path. If not, it moves them to
    the output path.

    :param temp_folder: str: Specify the folder that contains the files to be processed
    :param output_path: str: Specify the path where the files will be moved to
    """
    try:
        for root, dirs, files in os.walk(temp_folder):
            for file in files:
                source_file_path = os.path.join(root, file)

                if zipfile.is_zipfile(source_file_path):
                    extract_zip_recursive(source_file_path, output_path)
                else:
                    move_file(source_file_path, output_path)

    except Exception as e:
        logger.error(f"Error occurred during file processing: {e}")
        raise


def move_file(source_file_path: str, output_path: str):
    """
    The move_file function takes a source file path and an output path as arguments.
    It then splits the base name of the file from its extension, generates a unique suffix,
    and creates a new filename with that suffix appended to it. It then moves the original
    file to this new location.

    :param source_file_path: str: Specify the path of the file to be moved
    :param output_path: str: Specify the path to which the file will be moved
    :return: A boolean value indicating whether the file was successfully moved
    """
    try:
        base_name, extension = os.path.splitext(os.path.basename(source_file_path))
        unique_suffix = str(uuid.uuid4())[:8]
        new_file_name = f"{base_name}_{unique_suffix}{extension}"
        dest_file_path = os.path.join(output_path, new_file_name)
        shutil.move(source_file_path, dest_file_path)

    except Exception as e:
        logger.error(f"Error occurred during file moving: {e}")
        raise
