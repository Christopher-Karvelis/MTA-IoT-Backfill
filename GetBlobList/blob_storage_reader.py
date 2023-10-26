import datetime
import json
import os

from azure.storage.blob import BlobServiceClient


class BlobStorageWriter:
    def __init__(self):
        container_name = "axh-opcpublisher"
        connection_string = os.getenv("AzureWebJobsStorage")
        self.blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        self.container_client = self.blob_service_client.get_container_client(
            container=container_name
        )

    def list_blobs(self, ts_start, ts_end):
        blobs_with_start_to_consider = self.container_client.list_blobs(name_starts_with=ts_start[:10])
        return [blob["name"] for blob in blobs_with_start_to_consider if blob["name"].endswith("json")]
