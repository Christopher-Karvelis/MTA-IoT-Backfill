import io
import os
from io import BytesIO

from azure.storage.blob.aio import BlobServiceClient


def get_backfilling_container_client():
    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("AzureWebJobsStorage")
    )
    return blob_service_client.get_container_client(container="backfill")


def get_source_container_client():
    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("SOURCE_STORAGE_ACCOUNT_CONNECTION_STRING")
    )
    return blob_service_client.get_container_client("axh-opcpublisher")


async def download_string_blob(blob_name, container_client):
    blob_client = container_client.get_blob_client(blob_name)

    blob_response = await blob_client.download_blob()
    blob_content = await blob_response.readall()

    return io.StringIO(blob_content.decode("utf-8"))


async def download_blob_into_stream(blob_name, container_client):
    blob_client = container_client.get_blob_client(blob_name)

    blob_response = await blob_client.download_blob()
    bytes_io = io.BytesIO()

    await blob_response.readinto(bytes_io)
    return bytes_io


async def upload_parquet(container_client, group, parquet_blob_name):
    parquet_file = await _produce_parquet_bytes(group)
    await container_client.upload_blob(
        data=parquet_file, name=parquet_blob_name, overwrite=True
    )


async def _produce_parquet_bytes(group):
    parquet_file = BytesIO()
    group.to_parquet(parquet_file, coerce_timestamps="us", allow_truncated_timestamps=True)
    parquet_file.seek(0)
    return parquet_file
