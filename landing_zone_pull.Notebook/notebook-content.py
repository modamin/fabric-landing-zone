# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1cab98e2-fa62-450f-a279-bead862d9500",
# META       "default_lakehouse_name": "landigzone",
# META       "default_lakehouse_workspace_id": "74350445-e2ad-47ef-a70c-c1f103300b0e"
# META     }
# META   }
# META }

# CELL ********************

%pip install azure-storage-blob azure-identity

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContentSettings
import time
import os
import requests

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TODO: Replace <storage-account-name> with your actual storage account name
account_url = "https://aminazureml8689628918.blob.core.windows.net"

tenant_id = notebookutils.credentials.getSecret("https://aminfabricadminkv.vault.azure.net/", "tenant-id")
client_id = notebookutils.credentials.getSecret("https://aminfabricadminkv.vault.azure.net/", "client-id")
client_secret = notebookutils.credentials.getSecret("https://aminfabricadminkv.vault.azure.net/", "client-secret")


# Generates the access token for the Service Principal
credential = ClientSecretCredential(authority = 'https://login.microsoftonline.com/',
                                                        tenant_id = tenant_id,
                                                        client_id = client_id,
                                                        client_secret = client_secret)

# Create a blob service client to talk to the storage account
blob_service_client = BlobServiceClient(account_url, credential=credential)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# To check for no malware, get the Blob Index tags of the blob. Check that "Malware Scanning scan result" == "No threats found"

def check_no_malware(blob_service_client, container_name, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    tags = blob_client.get_blob_tags()
    for k, v in tags.items():
        if k == 'Malware Scanning scan result' and v == "No threats found":
            return True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Prepare the file path and file name for the dowloaded file

path = "https://data.bis.org/static/bulk/WS_LBS_D_PUB_csv_flat.zip"
file_name = path.replace("https://", "").replace(".","-").replace("/","-").replace("-zip",".zip")
print(file_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    # get the file using requests
    r = requests.get(path)
    if r.status_code == 200:

        #upload the file to blob storage container for malware scanning
        container_client = blob_service_client.get_container_client(container="input")
        blob_client = container_client.upload_blob(name=file_name, data=r.content, overwrite=True)

except Exception as e: print(e)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Malware scanning in Azure Blob Storage is near real-time. It happens as soon as the file is uploaded
# However, depending on the size of the file the scan itself might take some time
# Check every 10 seconds for 100 seconds if the file is scanned, then copy the file to the Files section of the lakehouse
for i in range(10):
    time.sleep(10)
    if check_no_malware(blob_service_client = blob_service_client, container_name = "input", blob_name = file_name):
        blob_client = blob_service_client.get_blob_client(container="input", blob=file_name)
        try:
            with open(f'/lakehouse/default/Files/{file_name}', 'wb') as f:
                download_stream = blob_client.download_blob()
                f.write(download_stream.readall())
        except Exception as e: print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
