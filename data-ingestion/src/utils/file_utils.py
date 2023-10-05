import os
import shutil
import json
import tempfile

logger = None


def save_to_file(file_path: str, content: str):
    """
    The save_to_file function takes in a file path and content to be written to the file.
    It then writes the content into the specified file path. If successful, it returns
    the same file path.

    :param file_path: str: Specify the file path where the content should be saved
    :param content: object: Specify the type of object that will be passed as an argument
    :return: The file path
    """
    try:
        with open(file_path, "w") as file:
            file.write(content)
        logger.info(f"File saved successfully to this {file_path}.")
        return file_path
    except Exception as e:
        logger.error(f"An error occurred while saving the file to this location {file_path}: {e}")
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


def read_json_file(file_path: str):
    """
    The read_json_file function reads a JSON file and returns the data as a Python dictionary.

    :param file_path: str: Specify the path to the json file
    :return: A dictionary
    """
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    except Exception as e:
        logger.error(f"Error occurred while reading the JSON file: {e}")
        raise


def remove_directory(directory_path: str):
    """
    The remove_directory function removes a directory and all of its contents.

    :param directory_path: str: Specify the path of the directory to be removed
    :return: True if the directory was removed, false otherwise
    """
    try:
        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)
            logger.info(f"Directory '{directory_path}' removed successfully.")
        else:
            logger.info(f"Directory '{directory_path}' does not exist.")
    except Exception as e:
        logger.error(f"Error removing directory '{directory_path}': {e}")
        raise


def get_file_paths(directory: str):
    """
    The get_file_paths function takes a directory as an argument and returns the file names in that directory.


    :param directory: str: Specify the directory to read file names from
    :return: A list of file names in the directory
    """
    try:
        file_names = []
        for root, directories, files in os.walk(directory):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                file_names.append(os.path.basename(file_path))
        return file_names
    except Exception as e:
        print(f"An error occurred while reading file names: {str(e)}")
        raise


def move_file(source_file: str, destination_folder: str):
    """
    The move_file function moves a file from one location to another.
        Args:
            source_file (str): The path of the file to be moved.
            destination_folder (str): The path of the folder where the file will be moved.

    :param source_file: str: Specify the source file path
    :param destination_folder: str: Specify the destination folder where the file will be moved
    """
    try:
        shutil.move(source_file, destination_folder)
        logger.info("File moved successfully.")
    except Exception as e:
        logger.error(f"An error occurred while moving file : {str(e)}")
        raise
