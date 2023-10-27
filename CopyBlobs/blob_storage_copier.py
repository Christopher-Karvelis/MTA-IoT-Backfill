import logging
import os

from azure.storage.blob import BlobServiceClient


class BlobStorageCopier:
    def __init__(self, source_container_name = "axh-opcpublisher", target_container_name="backfill"):
        self.source_container_name = source_container_name
        self.target_container_name = target_container_name
        connection_string = os.getenv("AzureWebJobsStorage")
        self.blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )

        self.source_container_client = self.blob_service_client.get_container_client(
            container=self.source_container_name
        )

        self.target_container_client = self.blob_service_client.get_container_client(
            container=target_container_name
        )

        try:
            self.target_container_client.create_container()
        except Exception as e:
            logging.info(
                f"Probably container {target_container_name} already exists, failed with Exception {e}"
            )

    def copy_blobs(self, ts_start, ts_end):
        blobs_with_start_to_consider = self.source_container_client.list_blobs(name_starts_with=ts_start[:10])
        for blob in blobs_with_start_to_consider:
            if blob["name"].endswith("json"):
                self._copy_blob(blob)

    def _copy_blob(self, blob):
        blob_url = self.source_container_client.get_blob_client(blob["name"]).url
        copy_blob = self.target_container_client.get_blob_client(blob["name"])
        copy_blob.start_copy_from_url(blob_url)



