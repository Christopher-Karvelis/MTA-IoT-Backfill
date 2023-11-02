import datetime
import logging
import os

from azure.storage.blob import (BlobClient, BlobSasPermissions,
                                BlobServiceClient, generate_blob_sas)


class BlobStorageCopier:
    def __init__(
        self, source_container_name="axh-opcpublisher", target_container_name="backfill"
    ):
        self.source_container_name = source_container_name
        self.target_container_name = target_container_name

        source_connection_string = os.getenv("SOURCE_STORAGE_ACCOUNT_CONNECTION_STRING")
        self.source_blob_service_client = BlobServiceClient.from_connection_string(
            source_connection_string
        )

        target_connection_string = os.getenv("AzureWebJobsStorage")
        self.target_blob_service_client = BlobServiceClient.from_connection_string(
            target_connection_string
        )

        self.source_container_client = (
            self.source_blob_service_client.get_container_client(
                container=self.source_container_name
            )
        )

        self.target_container_client = (
            self.target_blob_service_client.get_container_client(
                container=target_container_name
            )
        )

        try:
            self.target_container_client.create_container()
        except Exception as e:
            logging.info(
                f"Probably container {target_container_name} already exists, failed with Exception {e}"
            )

    def copy_blobs(self, ts_start, ts_end):
        blobs_with_start_to_consider = self.source_container_client.list_blobs(
            name_starts_with=ts_start[:10]
        )
        for blob in blobs_with_start_to_consider:
            if blob["name"].endswith("json"):
                self._copy_blob(blob)

    def _copy_blob(self, blob):
        source_blob_client = self.source_container_client.get_blob_client(blob["name"])
        sas_token = create_service_sas_blob(
            source_blob_client, self.source_blob_service_client.credential.account_key
        )
        blob_url = f"{source_blob_client.url}?{sas_token}"
        copy_blob = self.target_container_client.get_blob_client(blob["name"])
        copy_blob.start_copy_from_url(blob_url)


def create_service_sas_blob(blob_client: BlobClient, account_key: str):
    start_time = datetime.datetime.now(datetime.timezone.utc)
    expiry_time = start_time + datetime.timedelta(days=30)

    sas_token = generate_blob_sas(
        account_name=blob_client.account_name,
        container_name=blob_client.container_name,
        blob_name=blob_client.blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=expiry_time,
        start=start_time,
    )

    return sas_token
