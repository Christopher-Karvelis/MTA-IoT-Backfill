import asyncio
import io
import os

from azure.storage.blob.aio import BlobServiceClient


async def load_one_hour_of_data_starting_at(date, hour):
    # this should not be here...
    storage_options = {"connection_string": os.getenv("SOURCE_STORAGE_ACCOUNT_CONNECTION_STRING")}

    # put axh-opcpublisher instead of backfill for real stuff
    pattern_to_read = f"{date}/{hour}/01"

    result = await get_data(storage_options, pattern_to_read)
    return result


async def get_data(storage_options, pattern_to_read):
    blob_service_client = BlobServiceClient.from_connection_string(
        storage_options["connection_string"]
    )
    source_container_client = blob_service_client.get_container_client("backfill")

    # this sucks a bit, because by getting everything and then doing
    blobs_with_start_to_consider = source_container_client.list_blobs(name_starts_with=pattern_to_read)
    tasks = [download_blob(blob["name"], source_container_client) async for blob in blobs_with_start_to_consider if blob["name"].endswith("json")]
    results = await asyncio.gather(*tasks)
    return results


async def download_blob(blob_name, container_client):
    blob_client = container_client.get_blob_client(blob_name)

    blob_response = await blob_client.download_blob()
    blob_content = await blob_response.readall()
    return io.StringIO(blob_content.decode('utf-8'))
