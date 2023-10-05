from google.cloud import storage
import os

logger = None


def extract_bucket_name(bucket_url):
    """
    The extract_bucket_name function takes a bucket URL as input and returns the name of the bucket.

    :param bucket_url: Extract the bucket name from the url
    :return: The name of the bucket
    """
    try:
        bucket_name = bucket_url.split("//")[1].split("/")[0]
        return bucket_name
    except Exception as e:
        logger.error("Error occurred while extracting bucket name from url:", e)
        raise


def extract_bucket_path(bucket_url):
    """
    The extract_bucket_path function takes a bucket URL as input and returns the path to the bucket.

    :param bucket_url: Extract the bucket path
    :return: A string
    """
    try:
        bucket_path = "/".join(bucket_url.split("//")[1].split("/")[1:-1])
        return bucket_path
    except (IndexError, AttributeError) as e:
        logger.error("Error occurred while extracting bucket path from url :", e)
        raise


def extract_bucket_file(bucket_url):
    """
    The extract_bucket_file function takes a bucket url as input and returns the file name of the object in that
    bucket.

    :param bucket_url: Extract the bucket path from an url
    :return: The name of the file that is stored in the bucket
    """
    try:
        bucket_path = bucket_url.split("/")[-1]
        return bucket_path
    except (IndexError, AttributeError) as e:
        logger.error("Error occurred while extracting bucket file from url :", e)
        raise


def create_client(creds_json_path: str = None):
    """
    The create_client function creates a storage client object.
        Args:
            creds_json_path (str): The path to the credentials json file.

    :param creds_json_path: str: Specify the path to the credentials json file
    :return: A client object
    """
    try:
        if creds_json_path:
            client = storage.Client.from_service_account_json(creds_json_path)
            logger.info("Storage client created with credentials")
            return client
        else:
            client = storage.Client()
            logger.info("Storage client created without Credentials")
            return client
    except Exception as e:
        logger.error(f"Storage client not created : {e}")
        raise


def check_file_exists_in_path(
        bucket_name: str,
        bucket_path: str,
        file_name: str):
    """
    The check_file_exists_in_path function checks if a file exists in the specified path.
        Args:
            bucket_name (str): The name of the GCS bucket to check for existence of file.
            bucket_path (str): The path within the GCS bucket to check for existence of file.  If no value is provided,
            then it will default to root directory.
            file_name (str): The name of the GCS object/file that you are checking for existence in this location

    :param bucket_name: str: Specify the name of the bucket to check
    :param bucket_path: str: Specify the path of the file in gcs
    :param file_name: str: Specify the name of the file to be uploaded
    :return: A boolean value
    """
    try:
        client = create_client()
        bucket = client.get_bucket(bucket_name)

        if bucket_path:
            file_path = f"{bucket_path}/{file_name}"
        else:
            file_path = file_name

        blob = bucket.blob(file_path)
        blob_status = blob.exists()
        if blob_status:
            logger.info(
                f"file exists in this path {bucket_name + '/' + bucket_path + '/' + file_name}")
        else:
            logger.info(
                f"file not exists in this path {bucket_name + '/' + bucket_path + '/' + file_name}")
        return blob.exists()
    except Exception as e:
        logger.error(
            f"Error checking file existence {bucket_name + '/' + bucket_path + '/' + file_name} : {e}")
        raise


def download_file_from_path(
        bucket_name: str,
        bucket_path: str,
        file_name: str,
        local_path: str):
    """
    The download_file_from_path function downloads a file from GCS to the local filesystem.

    :param bucket_name: str: Specify the name of the bucket where you want to upload your file
    :param bucket_path: str: Specify the path to the file in gcs
    :param file_name: str: Specify the name of the file to be downloaded
    :param local_path: str: Specify the local directory where the file will be downloaded
    :return: A file object
    """
    try:
        client = create_client()
        bucket = client.get_bucket(bucket_name)

        if bucket_path:
            print(f"{bucket_path}/{file_name}")
            blob = bucket.blob(f"{bucket_path}/{file_name}")
        else:
            blob = bucket.blob(file_name)

        # Create the local directory if it doesn't exist
        os.makedirs(local_path, exist_ok=True)
        file_path = os.path.join(local_path, file_name)

        blob.download_to_filename(file_path)
        logger.info(
            f"File downloaded from gcs {bucket_name + '/' + bucket_path +'/'+ file_name} successfully to: {local_path}")
    except Exception as e:
        logger.error(
            f"Error downloading file from gcs {bucket_name + '/' + bucket_path + '/' + file_name} : {e}")
        raise


def list_files_in_path(bucket_name: str, prefix: str):
    """
    The list_files_in_path function lists all files in a specified path.

    :param bucket_name: str: Specify the bucket name
    :param prefix: str: Specify the folder in which to list the files
    :return: A list of file names in the specified path
    """
    try:
        # Instantiate the client
        client = create_client()

        # Get the bucket
        bucket = client.get_bucket(bucket_name)

        # List all blobs in the bucket with the specified prefix
        blobs = bucket.list_blobs(prefix=prefix)

        # Filter out folders and return the list of file names
        file_names = []
        for blob in blobs:
            if not blob.name.endswith('/'):
                # Remove the prefix to get only the file name
                file_name = blob.name.replace(prefix, '', 1)
                file_names.append(file_name.lstrip("/"))

        return file_names
    except Exception as e:
        logger.error(
            f"An error occurred while listing the files from {bucket_name + '/' + prefix}: {e}")
        raise


def upload_file_to_path(
        bucket_name: str,
        source_file_path: str,
        destination_blob_name: str):
    """
    The upload_file_to_path function uploads a file to the specified path in GCS.

    :param bucket_name: str: Specify the name of the bucket where you want to upload your file
    :param source_file_path: str: Specify the path of the file to be uploaded
    :param destination_blob_name: str: Specify the name of the file in gcs
    """
    try:
        # Instantiate the client
        client = create_client()

        # Get the bucket
        bucket = client.get_bucket(bucket_name)

        # Upload the file to the GCS bucket
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)

        logger.info(
            f"File {source_file_path} uploaded to {destination_blob_name}")
    except Exception as e:
        logger.error(f"An error occurred while uploading to path : {e}")
        raise


def delete_path(bucket_name: str, folder_path: str):
    """
    The delete_path function deletes a folder and all of its contents from the specified bucket.

    :param bucket_name: str: Specify the name of the bucket
    :param folder_path: str: Specify the path of the folder to be deleted
    :return: A boolean value
    """
    try:
        # Initialize the client
        client = create_client()

        # Get the bucket
        bucket = client.get_bucket(bucket_name)

        # List all objects in the folder
        blobs = bucket.list_blobs(prefix=folder_path)

        # Delete each object inside the folder
        for blob in blobs:
            blob.delete()

        logger.info(f"Folder '{folder_path}' deleted successfully.")
    except Exception as e:
        logger.error(f"An error occurred while deleting the folder: {e}")
        raise
