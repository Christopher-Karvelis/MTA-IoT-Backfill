import datetime
import json
import os

from azure.storage.blob import BlobServiceClient


class BlobStorageWriter:
    def __init__(self, logger):
        self.logger = logger
        container_name = os.getenv("AZURE_CONTAINER_NAME")
        connection_string = os.getenv("AzureWebJobsStorage")
        self.blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        self.container_client = self.blob_service_client.get_container_client(
            container=container_name
        )

        try:
            self.container_client.create_container()
        except Exception as e:
            self.logger.info(
                f"Probably container {container_name} already exists, failed with Exception {e}"
            )

    async def upload_blob(self, blob_data):

        blob_name = (
            f'{datetime.datetime.utcnow().strftime("%Y-%m-%d/%H/%M-%S-%f")[:-4]}.json'
        )
        data = json.dumps(blob_data)

        try:
            blob_client = self.container_client.get_blob_client(blob=blob_name)
            blob_client.upload_blob(data, blob_type="BlockBlob")
            self.logger.info(
                f"Successfully stored rejected signals in blob {blob_name}"
            )
        except Exception as e:
            raise Exception(f"could not stored rejected signals in blob {blob_name}", e)
