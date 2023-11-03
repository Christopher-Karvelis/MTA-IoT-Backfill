import io
import os

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
    # put axh-opcpublisher instead of backfill for real stuf
    return blob_service_client.get_container_client("backfill")


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
