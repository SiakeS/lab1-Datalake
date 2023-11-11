import os
import zipfile

from azure.storage.blob import BlobClient

conn_string = "DefaultEndpointsProtocol=https;AccountName=pythonazurestorage04185;AccountKey=YU+62+b9aSB+xytW807lksoCZgCAgxtlAuzumXxO9LJM9A7OyOJphiTEiDHn3w179pbVo4gIfExz+ASt/IL73Q==;EndpointSuffix=core.windows.net"
local_path = "/Users/siakesophie/Local_Docs/ING_3/S9/Data Lake/labs/data"

# Function to extract all files from zipped files in a directory 
def extract_zip_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".zip"):
                with zipfile.ZipFile(os.path.join(root, file), "r") as zip_ref:
                    zip_ref.extractall(root)

# Function to create a list of all the recursive files in a directory
def list_files(directory):
    file_list = []
    extract_zip_files(directory)
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_list.append(os.path.join(root, file))
    return file_list

# List all files in the directory
local_files = list_files(local_path)

# The upload of files in the blob container
for local_file in local_files:

    file_name = os.path.basename(local_file)

    blob_client = BlobClient.from_connection_string(
        conn_string,
        container_name="blob-container-01",
        blob_name=file_name,
    )

    with open(os.path.join(local_path, local_file), "rb") as data:
        blob_client.upload_blob(data)
    
        print(f"Uploaded {local_file} to {blob_client.url}")
