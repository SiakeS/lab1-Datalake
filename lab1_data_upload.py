import os, random, zipfile
from azure.storage.filedatalake import DataLakeServiceClient

local_path = "/Users/siakesophie/Local_Docs/ING_3/S9/Data Lake/labs/lab1/data"
account_name = os.getenv('STORAGE_ACCOUNT_NAME', "")
account_key = os.getenv('STORAGE_ACCOUNT_KEY', "")

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


def upload_files():
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https", account_name), credential=account_key)
    file_system_name = f"lab1-data-{random.randint(1,100000):05}"
    file_system_client = service_client.create_file_system(file_system=file_system_name)
    local_files = list_files(local_path)

    for local_file in local_files:

        file_name = os.path.basename(local_file)

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https",
            account_name
        ), credential=account_key)

        with open(os.path.join(local_path, local_file), "rb") as data:
            file_content = data.read()
            # Ensures that the uploaded file has the same name as the local file
            file_path = file_name
            file_client = file_system_client.get_file_client(file_path)

            file_client.create_file()
            file_client.append_data(file_content, offset=0)
            file_client.flush_data(len(file_content))
        
            print(f"Upload completed for {local_file}")

if __name__ == "__main__":
    upload_files()
