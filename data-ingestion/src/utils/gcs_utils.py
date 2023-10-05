from google.cloud import storage
import os
import uuid

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
            return client
        else:
            client = storage.Client()
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
            f"File downloaded from gcs {bucket_name + '/' + bucket_path + '/' + file_name} successfully to: {local_path}")
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
    except Exception as e:
        logger.error(f"An error occurred while deleting the folder: {e}")
        raise


def list_files_from_gcs_uri(gcs_uri):
    """
    The list_files_from_gcs_uri function takes a GCS URI as input and returns a list of all files in the bucket.

    :param gcs_uri: Specify the gcs path from which files are to be listed
    :return: A list of all the files in the given gcs path
    """
    try:
        # Remove 'gs://' prefix and split the remaining string into bucket name and prefix
        input_parts = gcs_uri[len('gs://'):].split('/')

        # Extract bucket name and prefix
        bucket_name = input_parts[0]
        prefix = '/'.join(input_parts[1:])

        # Create a storage client
        client = storage.Client()

        # Get the bucket
        bucket = client.get_bucket(bucket_name)

        # List all blobs (files) with the given prefix in the bucket
        blobs = bucket.list_blobs(prefix=prefix)

        # Store the file paths in a list
        file_paths = [f'gs://{bucket_name}/{blob.name}' for blob in blobs]

        return file_paths
    except Exception as e:
        logger.error(f"Error occurred while listing files from gcs path : {e}")
        raise


def download_from_gcs(gcs_url):
    """
    The download_from_gcs function downloads a file from Google Cloud Storage to the local filesystem.

    :param gcs_url: Specify the gcs url of the file to be downloaded
    :return: A local file path
    """
    try:
        # Create a Google Cloud Storage client
        client = create_client()

        # Split the GCS URL into bucket and object names
        bucket_name, object_name = gcs_url.replace('gs://', '').split('/', 1)

        # Get the bucket and blob
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(object_name)

        temp_dir = f'temp/{str(uuid.uuid4())[:8]}'

        # Create the local file path in the temp dir
        local_path = os.path.join(temp_dir, object_name)

        # Create the local path if it does not exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Download the blob into the local file
        blob.download_to_filename(local_path)

        return local_path
    except Exception as e:
        print(f"Error while downloading the file from bucket url : {e}")
        raise


def upload_file_to_gcs(local_file_path, gcs_bucket_url):
    """
    The upload_file_to_gcs function uploads a file to Google Cloud Storage.

    :param local_file_path: Specify the path of the file to be uploaded
    :param gcs_bucket_url: Specify the gcs bucket url where the file will be uploaded
    """
    try:
        # Create a storage client
        client = storage.Client()

        # Get the bucket name and file path from the GCS bucket URL
        bucket_name, gcs_file_path = gcs_bucket_url.split('/', 2)[2].split('/', 1)

        # Get the bucket reference
        bucket = client.bucket(bucket_name)

        # Create a blob object with the GCS file path
        blob = bucket.blob(gcs_file_path)

        # Upload the file to GCS
        blob.upload_from_filename(local_file_path)

        print(f"File uploaded to GCS: {gcs_bucket_url}")
    except Exception as e:
        print(f"Error occurred while uploading file to gcs : {e}")
        raise


def copy_file(source_uri, destination_uri):
    try:
        # Instantiate a client
        client = storage.Client()

        # Get the source bucket and file name from the source URI
        source_bucket_name, source_blob_name = source_uri.split("/", 3)[2:4]

        # Get the destination bucket and file name from the destination URI
        destination_bucket_name, destination_blob_name = destination_uri.split("/", 3)[2:4]

        # Get the source bucket
        source_bucket = client.get_bucket(source_bucket_name)

        # Get the source blob
        source_blob = source_bucket.blob(source_blob_name)

        # Get the destination bucket
        destination_bucket = client.get_bucket(destination_bucket_name)

        # Copy the source blob to the destination bucket
        destination_blob = source_bucket.copy_blob(
            source_blob, destination_bucket, new_name=destination_blob_name
        )

        logger.info(
            f"File {source_bucket_name}/{source_blob_name} copied to {destination_bucket_name}/{destination_blob_name}"
        )
    except Exception as e:
        logger.error(f"Error occurred while copying file : {e}")
        raise


def save_text_to_gcs_file(file_uri, content):
    # Extract the bucket and file name from the file URI
    bucket_name, file_name = file_uri.replace("gs://", "").split("/", 1)

    # Instantiate a client
    client = storage.Client()

    # Get the bucket
    bucket = client.get_bucket(bucket_name)

    # Create a new blob (file) in the bucket
    blob = bucket.blob(file_name)

    # Upload the content to the blob
    blob.upload_from_string(content)